import dis
import sys
from types import CodeType
import typing as t

from ddtrace.internal.injection import HookType
from ddtrace.internal.test_visibility.coverage_lines import CoverageLines


# This is primarily to make mypy happy without having to nest the rest of this module behind a version check
# NOTE: the "prettier" one-liner version (eg: assert (3,11) <= sys.version_info < (3,12)) does not work for mypy
assert sys.version_info >= (3, 10) and sys.version_info < (3, 11)  # nosec

EXTENDED_ARG = dis.EXTENDED_ARG
LOAD_CONST = dis.opmap["LOAD_CONST"]
CALL = dis.opmap["CALL_FUNCTION"]
POP_TOP = dis.opmap["POP_TOP"]
IMPORT_NAME = dis.opmap["IMPORT_NAME"]
IMPORT_FROM = dis.opmap["IMPORT_FROM"]

JUMPS = set(dis.hasjabs + dis.hasjrel)
ABSOLUTE_JUMPS = set(dis.hasjabs)
BACKWARD_JUMPS = set(op for op in dis.hasjrel if "BACKWARD" in dis.opname[op])
FORWARD_JUMPS = set(op for op in dis.hasjrel if "BACKWARD" not in dis.opname[op])


def instrument_all_lines(code: CodeType, hook: HookType, path: str, package: str) -> t.Tuple[CodeType, CoverageLines]:
    new_code, new_consts, new_linetable, seen_lines = instrument_all_lines_nonrecursive(code, hook, path, package)

    # Instrument nested code objects recursively.
    for const_index, nested_code in enumerate(code.co_consts):
        if isinstance(nested_code, CodeType):
            new_consts[const_index], nested_lines = instrument_all_lines(nested_code, hook, path, package)
            seen_lines.update(nested_lines)

    return (
        code.replace(
            co_code=bytes(new_code),
            co_consts=tuple(new_consts),
            co_linetable=bytes(new_linetable),
            co_stacksize=code.co_stacksize + 4,  # TODO: Compute the value!
        ),
        seen_lines,
    )


# This function returns a modified version of the bytecode for the given code object, such that the beginning of
# every new source code line is prepended a call to the hook function with an argument representing the line number,
# path and dependency information of the corresponding source code line.
#
# The hook function is added to the code constants. For each line, a new constant of the form (line, path,
# dependency_info) is also added to the code constants, and then a call to the hook with that constant is added to
# the bytecode. For example, let's say the hook function is added to the code constants at index 100, and the
# instructions corresponding to source code line 42 in file "foo.py" are:
#
#    1000 LOAD_CONST    1
#    1002 RETURN_VALUE  0
#
# Then an object of the form (42, "foo.py", None) would be added to the constants (let's say at index 101), and the
# resulting instrumented code would look like:
#
#    2000 LOAD_CONST 100    # index of the hook function
#    2002 LOAD_CONST 101    # index of the (42, "foo.py", None) object
#    2004 CALL 1            # call the hook function with the 1 provided argument
#    2006 POP_TOP 0         # discard the return value
#
#    2008 LOAD_CONST 1      # (original instructions)
#    2010 RETURN_VALUE 0    #
#
# Because the instruction offsets will change, jump targets have to be updated in the new bytecode. This is achieved
# in the following way:
#
#  - As we iterate through the code, we keep a dictionary `new_offsets` mapping the old offsets to the new ones.
#    If any instrumentation instructions have been prepended to the instruction, the new offset points to the
#    beginning of the instrumentation, not the instruction itself. In the example above, new_offsets[1000] would
#    be 2000, not 2008. This is so that any jump to the instruction will jump to the hook call instead.
#
#  - When we find a jump instruction, we update a dictionary `old_targets` mapping the (old) offset of the jump
#    instruction to the (old) offset of the target instruction. In the new bytecode, the jump will be emitted with a
#    placeholder jump target of zero, since we don't know the final locations of all instructions yet.
#
#  - After the new bytecode with placeholders has been generated for the whole code (and all new instruction offsets
#    are known), all the jump target placeholders are updated with the final offsets of the targets.
#
# One complicating factor here are EXTENDED_ARG instructions. Every bytecode instruction has exactly two bytes, one
# for the instruction opcode and one for the argument. If the argument (such as a jump target) exceeds 255, one or
# more EXTENDED_ARG instructions are prepended to the instruction to provide the extra bytes of the argument. For
# example, a jump to 131844 (0x020304) would look like this:
#
#         EXTENDED_ARG 2
#         EXTENDED_ARG 3
#         JUMP_ABSOLUTE 4
#
# When we see EXTENDED_ARG instructions, we have to accumulate the values of their arguments and combine them with
# the value of the next non-EXTENDED_ARG instruction. The `extended_arg` variable is used for that.
#
# To avoid having to deal with a variable number of EXTENDED_ARGs when patching jump target placeholders, all jump
# instructions in the generated bytecode are emitted with 3 EXTENDED_ARGs, like:
#
#         EXTENDED_ARG 0
#         EXTENDED_ARG 2
#         EXTENDED_ARG 3
#         JUMP_ABSOLUTE 4
#
# even if some of them are not strictly needed and will be left with a 0 argument. In this way, the code can be
# written as a two-step procedure: generating the bytecode with placeholders (without having to know how many
# EXTENDED_ARGs will be needed for the final offsets beforehand), then patching the placeholders (which are
# guaranteed not to change size during patching), which makes the code simpler and more efficient.
#
# Another mapping that is kept for jumps is `new_ends`, which maps old offsets to the new offset of the _end_ of the
# whole instrumented block of instructions (i.e., the offset of the instruction just after the jump). This is so
# that we can find the jump instruction itself in the new code (skipping the inserted instrumentation), and also
# because relative jumps are relative to the offset of the _next_ instruction.


def instrument_all_lines_nonrecursive(
    code: CodeType, hook: HookType, path: str, package: str
) -> t.Tuple[bytearray, t.List[object], bytearray, CoverageLines]:
    old_code = code.co_code
    new_code = bytearray()
    new_linetable = bytearray()

    previous_line = code.co_firstlineno
    previous_line_new_offset = 0
    previous_previous_line = code.co_firstlineno

    arg = 0
    previous_arg = 0
    previous_previous_arg = 0
    extended_arg = 0

    current_import_name: t.Optional[str] = None
    current_import_package: t.Optional[str] = None

    new_offsets: t.Dict[int, int] = {}
    new_ends: t.Dict[int, int] = {}
    old_targets: t.Dict[int, int] = {}

    line_starts = dict(dis.findlinestarts(code))

    new_consts = list(code.co_consts)
    hook_index = len(new_consts)
    new_consts.append(hook)

    seen_lines = CoverageLines()
    is_first_instrumented_module_line = code.co_name == "<module>"

    def append_instruction(opcode: int, extended_arg: int) -> None:
        """
        Append an operation and its argument to the new bytecode.

        If the argument does not fit in a single byte, EXTENDED_ARG instructions are prepended as needed.
        """
        if extended_arg > 255:
            extended_bytes = (extended_arg.bit_length() - 1) // 8
            shift = 8 * extended_bytes

            while shift:
                new_code.append(EXTENDED_ARG)
                new_code.append((extended_arg >> shift) & 0xFF)
                shift -= 8

        new_code.append(opcode)
        new_code.append(extended_arg & 0xFF)

    def update_linetable(offset_delta: int, line_delta: int) -> None:
        """
        Add a new line number update to the line table array.

        Conceptually, the line table registers which ranges of offsets are assigned a given line number. Since we need
        to know the start and end offsets of a given line, this function should be called _after_ a given line is
        finished, i.e., at the beginning of the next line, and after all lines have been processed.

        See <https://github.com/python/cpython/blob/3.10/Objects/lnotab_notes.txt> for details on the line table format.
        """
        # Offset delta is always positive. If the delta is more than 254, we keep adding increments of 254 with 0 line
        # change, until we reach the desired number.
        while offset_delta > 254:
            new_linetable.append(254)
            new_linetable.append(0)
            offset_delta -= 254

        new_linetable.append(offset_delta)

        # Line delta can be either positive or negative. If it's greater than 127 (or less than -127), we keep adding
        # increments of 127 or -127 with 0 offset change, until we reach the desired number.
        while line_delta > 127:
            new_linetable.append(127)  # line_delta
            new_linetable.append(0)  # offset_delta
            line_delta -= 127

        while line_delta < -127:
            new_linetable.append(0x81)  # line_delta
            new_linetable.append(0)  # offset_delta
            line_delta += 127

        # Finally, append anything left from the line delta.
        new_linetable.append(line_delta & 0xFF)

    for old_offset in range(0, len(old_code), 2):
        opcode = old_code[old_offset]
        arg = old_code[old_offset + 1] | extended_arg

        new_offset = len(new_code)
        new_offsets[old_offset] = new_offset

        line = line_starts.get(old_offset)
        if line is not None:
            if old_offset > 0:
                # Beginning of new line: update line table entry for _previous_ line.
                update_linetable(new_offset - previous_line_new_offset, previous_line - previous_previous_line)
            previous_previous_line = previous_line
            previous_line = line
            previous_line_new_offset = new_offset

            seen_lines.add(line)

            append_instruction(LOAD_CONST, hook_index)
            append_instruction(LOAD_CONST, len(new_consts))
            # DEV: Because these instructions have fixed arguments and don't need EXTENDED_ARGs, we append them directly
            #      to the bytecode here. This loop runs for every instruction in the code to be instrumented, so this
            #      has some impact on execution time.
            new_code.append(CALL)
            new_code.append(1)
            new_code.append(POP_TOP)
            new_code.append(0)

            # Make sure that the current module is marked as depending on its own package by instrumenting the
            # first executable line.
            package_dep = None
            if is_first_instrumented_module_line:
                package_dep = (package, ("",))
                is_first_instrumented_module_line = False

            new_consts.append((line, path, package_dep))

        if opcode == EXTENDED_ARG:
            extended_arg = arg << 8
            continue

        extended_arg = 0

        if opcode in JUMPS:
            if opcode in ABSOLUTE_JUMPS:
                target = arg * 2
            elif opcode in FORWARD_JUMPS:
                target = old_offset + 2 + (arg * 2)
            elif opcode in BACKWARD_JUMPS:
                target = old_offset + 2 - (arg * 2)
            else:
                raise NotImplementedError(f"Unexpected instruction {opcode}")

            old_targets[old_offset] = target

            # Emit jump with placeholder 0 0 0 0 jump target.
            new_code.append(EXTENDED_ARG)
            new_code.append(0)
            new_code.append(EXTENDED_ARG)
            new_code.append(0)
            new_code.append(EXTENDED_ARG)
            new_code.append(0)
            new_code.append(opcode)
            new_code.append(0)

            new_ends[old_offset] = len(new_code)

        else:
            append_instruction(opcode, arg)

            # Track imports names
            if opcode == IMPORT_NAME:
                import_depth = code.co_consts[previous_previous_arg]
                current_import_name = code.co_names[arg]
                # Adjust package name if the import is relative and a parent (ie: if depth is more than 1)
                current_import_package = (
                    ".".join(package.split(".")[: -import_depth + 1]) if import_depth > 1 else package
                )
                new_consts[-1] = (
                    new_consts[-1][0],
                    new_consts[-1][1],
                    (current_import_package, (current_import_name,)),
                )

            # Also track import from statements since it's possible that the "from" target is a module, eg:
            # from my_package import my_module
            # Since the package has not changed, we simply extend the previous import names with the new value
            if opcode == IMPORT_FROM:
                import_from_name = f"{current_import_name}.{code.co_names[arg]}"
                new_consts[-1] = (
                    new_consts[-1][0],
                    new_consts[-1][1],
                    (new_consts[-1][2][0], tuple(list(new_consts[-1][2][1]) + [import_from_name])),
                )

        previous_previous_arg = previous_arg
        previous_arg = arg

    # Update line table for the last line we've seen.
    update_linetable(len(new_code) - previous_line_new_offset, previous_line - previous_previous_line)

    # Fixup the offsets.
    for old_offset, old_target in old_targets.items():
        new_offset = new_offsets[old_offset]
        new_target = new_offsets[old_target]
        new_end = new_ends[old_offset]
        opcode = old_code[old_offset]

        if opcode in ABSOLUTE_JUMPS:
            arg = new_target // 2
        elif opcode in FORWARD_JUMPS:
            arg = (new_target - new_end) // 2
        elif opcode in BACKWARD_JUMPS:
            arg = (new_end - new_target) // 2
        else:
            raise NotImplementedError(f"Unexpected instruction {opcode}")

        # The code to patch looks like <EXTENDED_ARG, 0, EXTENDED_ARG, 0, EXTENDED_ARG, 0, opcode, 0>.
        # Write each byte of the argument over the corresponding 0, starting from the end of the instruction.
        arg_offset = new_end - 1
        while arg:
            new_code[arg_offset] = arg & 0xFF
            arg >>= 8
            arg_offset -= 2

    return new_code, new_consts, new_linetable, seen_lines
