from abc import ABC
from dataclasses import dataclass
import dis
from enum import Enum
from types import CodeType
import typing as t

from .opcodes import *


HookType = t.Callable[[t.Any], t.Any]
EMPTY_BYTECODE = bytes([RESUME, 0, LOAD_CONST, 0, RETURN_VALUE, 0])
SKIP_LINES = frozenset([dis.opmap["END_ASYNC_FOR"]])


class JumpDirection(int, Enum):
    FORWARD = 1
    BACKWARD = -1

    @classmethod
    def from_opcode(cls, opcode: int) -> "JumpDirection":
        return cls.BACKWARD if "BACKWARD" in dis.opname[opcode] else cls.FORWARD


@dataclass
class JumpType(ABC):
    start: int
    arg: int
    direction: JumpDirection

    @property
    def end(self):
        return self.start + (self.arg << 1) * self.direction + 2


class Jump(ABC):
    def __init__(self, start: int, arg: int) -> None:
        self.start = start
        self.end: t.Optional[int] = None
        self.arg = arg

    def shift_foward(self, bytecode_lenght: int):
        assert self.end is not None
        self.end = self.start + (self.arg << 1) * self.direction + 2
        self.end += bytecode_lenght * self.direction
        self.arg = ((self.end - self.start) >> 1) * self.direction - 2


class RJump(Jump):
    __opcodes__ = set(dis.hasjrel)

    def __init__(self, start: int, arg: int, direction: JumpDirection) -> None:
        super().__init__(start, arg)

        self.direction = direction
        self.end = start + (self.arg << 1) * self.direction + 2


class Instruction:
    __slots__ = ("offset", "opcode", "arg", "targets")

    def __init__(self, offset: int, opcode: int, arg: int) -> None:
        self.offset = offset
        self.opcode = opcode
        self.arg = arg
        self.targets: t.List["Branch"] = []


class Branch:
    def __init__(self, start: Instruction, end: Instruction) -> None:
        self.start = start
        self.end = end

    @property
    def arg(self) -> int:
        return abs(self.end.offset - self.start.offset - 2) >> 1


EXTENDED_ARG = dis.EXTENDED_ARG
NO_OFFSET = -1


def inject_co_consts(consts: t.List, *args) -> t.Tuple[int, ...]:
    injection_indexes = []
    for a in args:
        injection_indexes.append(len(consts))
        consts.append(a)

    return tuple(injection_indexes)


def inject_co_varnames(vars: t.List, *args) -> t.Tuple[int, ...]:
    injection_indexes = []
    for a in args:
        injection_indexes.append(len(vars))
        vars.append(a)

    return tuple(injection_indexes)


def inject_co_names(names: t.List, *args: str) -> t.Tuple[int, ...]:
    injection_indexes = []
    for a in args:
        injection_indexes.append(len(names))
        names.append(a)

    return tuple(injection_indexes)


def instr_with_arg(opcode: int, arg: int) -> t.List[Instruction]:
    instructions = [Instruction(NO_OFFSET, opcode, arg & 0xFF)]
    arg >>= 8
    while arg:
        instructions.insert(0, Instruction(NO_OFFSET, EXTENDED_ARG, arg & 0xFF))
        arg >>= 8
    return instructions


def instructions_to_bytecode(instructions: t.List[Instruction]) -> bytes:
    new_code = bytearray()
    for instr in instructions:
        new_code.append(instr.opcode)
        if instr.opcode > dis.HAVE_ARGUMENT:
            new_code.append(instr.arg)
        else:
            new_code.append(0)

    return bytes(new_code)


@dataclass
class ExceptionTableEntry:
    start: int
    end: int
    target: int
    depth_lasti: int

    def is_entirely_before(self, index: int) -> bool:
        return self.end <= index and self.target <= index and self.start <= index


def parse_exception_table(code: CodeType):
    iterator = iter(code.co_exceptiontable)
    try:
        while True:
            start = _from_varint(iterator) << 1
            length = _from_varint(iterator) << 1
            end = start + length - 2  # Present as inclusive, not exclusive
            target = _from_varint(iterator) << 1
            dl = _from_varint(iterator)
            yield ExceptionTableEntry(start, end, target, dl)
    except StopIteration:
        return


def _from_varint(iterator: t.Iterator[int]) -> int:
    b = next(iterator)
    val = b & 63
    while b & 64:
        val <<= 6
        b = next(iterator)
        val |= b & 63
    return val


def _to_varint(value: int, set_begin_marker: bool = False) -> bytes:
    # Encode value as a varint on 7 bits (MSB should come first) and set
    # the begin marker if requested.
    temp = bytearray()
    if value < 0:
        raise ValueError("Invalid value for varint")
    while value:
        temp.insert(0, value & 63 | (64 if temp else 0))
        value >>= 6
    temp = temp or bytearray([0])
    if set_begin_marker:
        temp[0] |= 128
    return bytes(temp)


@dataclass
class InjectionContext:
    code: CodeType
    instructions: t.List[Instruction]
    injection_offsets: t.List[int]
    consts: t.List[t.Any]
    names: t.List[str]
    variables: t.List[str]


def inject_instructions_alt(injection_context: InjectionContext) -> CodeType:
    instructions: t.List[Instruction] = []
    code = injection_context.code

    # Find the offset of the RESUME opcode. We should not add any instrumentation before this point.
    resume_offset = NO_OFFSET
    for i in range(0, len(code.co_code), 2):
        if code.co_code[i] == RESUME:
            resume_offset = i
            break
    if resume_offset == NO_OFFSET:
        return code

    try:
        code_iter = iter(enumerate(code.co_code))
        ext: list[int] = []
        while True:
            original_offset, opcode = next(code_iter)
            _, arg = next(code_iter)

            if original_offset in injection_context.injection_offsets:
                instructions.extend(injection_context.instructions)

            if opcode == EXTENDED_ARG:
                ext.append(arg)
                continue

            # We group all the extended args in the same logical instruction to simplify adjustments.
            if ext:
                ext_arg = 0
                for e in reversed(ext):
                    ext_arg = ext_arg << 8 + e
                arg = ext_arg << 8 + arg

            instructions.append(Instruction(NO_OFFSET, opcode, arg))

    except StopIteration:
        pass

    # fix
    #   - jumps
    #   - exceptions
    #   - line numbers

    return code


def inject_instructions(
    code: CodeType, inject_instructions: t.List[Instruction], injection_indexes: t.List[int]
) -> CodeType:
    # TODO[perf]: Check if we really need to << and >> everywhere
    # trap_func, trap_arg = hook, path

    # THIS WAS AN ATTEMPT AT AN ALTERNATIVE APPROACH

    instructions: t.List[Instruction] = []

    # seen_lines = CoverageLines()

    exc_table = list(parse_exception_table(injection_context.code))
    exc_table_offsets = {_ for e in exc_table for _ in (e.start, e.end, e.target)}
    offset_map = {}

    # Collect all the original jumps.
    jumps: t.Dict[int, Jump] = {}
    jumps_by_index: t.Dict[int, JumpType] = {}
    injection_points: t.Dict[int, int] = {}  # DEV: This uses the original offsets
    line_map = {}
    line_starts = dict(dis.findlinestarts(injection_context.code))

    # Find the offset of the RESUME opcode. We should not add any instrumentation before this point.
    resume_offset = NO_OFFSET
    for i in range(0, len(code.co_code), 2):
        if code.co_code[i] == RESUME:
            resume_offset = i
            break

    # # If we are looking at an empty module, we trick ourselves into instrumenting line 0 by skipping the RESUME at index
    # # and instrumenting the second offset:
    # if code.co_name == "<module>" and line_starts == {0: 0} and code.co_code == EMPTY_BYTECODE:
    #     line_starts = {2: 0}

    # The previous two arguments are kept in order to track the depth of the IMPORT_NAME
    # For example, from ...package import module
    current_arg: int = 0
    previous_arg: int = 0
    _previous_previous_arg: int = 0
    current_import_name: t.Optional[str] = None
    current_import_package: t.Optional[str] = None
    # There can be multiple conditions why an injection index cannot be honored. Here we store honored injection indexes.
    actual_injection_indexes = []

    try:
        code_iter = iter(enumerate(code.co_code))
        ext: list[bytes] = []
        injected_instructions_total_count = 0
        while True:
            original_offset, opcode = next(code_iter)

            if original_offset in exc_table_offsets:
                offset_map[original_offset] = len(instructions) << 1

            is_code_injected = False

            if original_offset in injection_indexes and original_offset > resume_offset:
                line = line_starts[original_offset]
                if code.co_code[original_offset] not in SKIP_LINES:
                    # Inject trap call at the beginning of the line. Keep
                    # track of location and size of the trap call
                    # instructions. We need this to adjust the location
                    # table.
                    # trap_instructions = trap_call(trap_index, len(new_consts))
                    injection_points[original_offset] = len(inject_instructions)
                    instructions.extend(inject_instructions)
                    is_code_injected = True
                    actual_injection_indexes.append(original_offset)

                    # # Make sure that the current module is marked as depending on its own package by instrumenting the
                    # # first executable line
                    # package_dep = None
                    # if code.co_name == "<module>" and len(new_consts) == len(code.co_consts) + 1:
                    #     package_dep = (package, ("",))

                    # new_consts.append((line, trap_arg, package_dep))

                    line_map[original_offset] = instructions[0]

                # seen_lines.add(line)

            _, arg = next(code_iter)

            offset = len(instructions) << 1  # opcode + arg

            # # Let's adjust right now the exception table
            # for exc_entry in exc_table:
            #     if exc_entry.is_entirely_before(original_offset):
            #         continue
            #     elif is_injection_index:
            #         if original_offset >= exc_entry.start and original_offset < exc_entry.end:

            # Propagate code
            instructions.append(Instruction(NO_OFFSET, opcode, arg))
            # instructions.append(Instruction(original_offset, opcode, arg))

            if opcode is EXTENDED_ARG:
                ext.append(arg)
                continue

            _previous_previous_arg = previous_arg
            previous_arg = current_arg
            current_arg = int.from_bytes([*ext, arg], "big", signed=False)
            ext.clear()

            # # Track imports names
            # if opcode == IMPORT_NAME:
            #     import_depth = code.co_consts[_previous_previous_arg]
            #     current_import_name = code.co_names[current_arg]
            #     # Adjust package name if the import is relative and a parent (ie: if depth is more than 1)
            #     current_import_package = (
            #         ".".join(package.split(".")[: -import_depth + 1]) if import_depth > 1 else package
            #     )
            #     new_consts[-1] = (
            #         new_consts[-1][0],
            #         new_consts[-1][1],
            #         (current_import_package, (current_import_name,)),
            #     )

            # # Also track import from statements since it's possible that the "from" target is a module, eg:
            # # from my_package import my_module
            # # Since the package has not changed, we simply extend the previous import names with the new value
            # if opcode == IMPORT_FROM:
            #     import_from_name = f"{current_import_name}.{code.co_names[current_arg]}"
            #     new_consts[-1] = (
            #         new_consts[-1][0],
            #         new_consts[-1][1],
            #         (new_consts[-1][2][0], tuple(list(new_consts[-1][2][1]) + [import_from_name])),
            #     )

            # Collect branching instructions for processing
            if opcode in RJump.__opcodes__:
                jumps[len(instructions) - 1] = RJump(original_offset, current_arg, JumpDirection.from_opcode(opcode))
                jumps_by_index[offset] = JumpType(original_offset, arg, JumpDirection.from_opcode(opcode))

    except StopIteration:
        pass

    # OPTION 1
    # Adjust the destination of the jump instructions
    injected_bytecode_len = len(inject_instructions) << 1  # opcode + arg
    for instr_idx, j in jumps.items():
        instruction = instructions[instr_idx]
        bytecode_idx = instr_idx * 2
        delta = j.arg * 2 + 2
        for injection_cycle, inject_index in enumerate([ii for ii in injection_indexes if ii >= bytecode_idx]):
            adjusted_injection_idx = inject_index + injection_cycle * injected_bytecode_len
            if bytecode_idx + delta < adjusted_injection_idx:
                # no need to make an adjustment
                continue

            delta += injected_bytecode_len
        # TODO: Account for backward jumps
        instruction.arg = (delta - 2) >> 1

    # OPTION 2
    # Adjust the destination of the jump instructions, exceptions, table, line tables
    original_start_of_block = 0
    original_end_of_block = 0
    for injection_cycle, inject_index in enumerate(actual_injection_indexes):
        original_start_of_block += injection_cycle * injected_bytecode_len
        original_end_of_block = inject_index + injection_cycle * injected_bytecode_len
        for instr_idx, jump in jumps_by_index.items():
            instruction = instructions[instr_idx]
            new_start = instr_idx << 1
            if jump.start < inject_index:
                new_arg = instruction.arg

        # # Collect all the old jump start and end offsets
        # jump_targets = {_ for j in jumps.values() for _ in (j.start, j.end)}

        # # Adjust all the offsets and map the old offsets to the new ones for the
        # # jumps
        # for index, instr in enumerate(instructions):
        #     new_offset = index << 1
        #     if instr.offset in jump_targets or instr.offset in offset_map:
        #         offset_map[instr.offset] = new_offset
        #     instr.offset = new_offset

        # # Adjust all the jumps, neglecting any EXTENDED_ARGs for now
        # branches: t.List[Branch] = []
        # for jump in jumps.values():
        #     new_start = offset_map[jump.start]
        #     new_end = offset_map[jump.end]

        #     # If we are jumping at the beginning of a line, jump to the
        #     # beginning of the trap call instead
        #     target_instr = line_map.get(jump.end, instructions[new_end >> 1])
        #     branch = Branch(instructions[new_start >> 1], target_instr)
        #     target_instr.targets.append(branch)

        #     branches.append(branch)

        # # Resolve the exception table
        # for e in exc_table:
        #     e.start = instructions[offset_map[e.start] >> 1]
        #     e.end = instructions[offset_map[e.end] >> 1]
        #     e.target = instructions[offset_map[e.target] >> 1]

        # # Process all the branching instructions to adjust the arguments. We
        # # need to add EXTENDED_ARGs if the argument is too large.
        # process_branches = True
        # exts: t.List[t.Tuple[Instruction, int]] = []
        # while process_branches:
        #     process_branches = False
        #     for branch in branches:
        #         jump_instr = branch.start
        #         new_arg = branch.arg
        #         jump_instr.arg = new_arg & 0xFF
        #         new_arg >>= 8
        #         c = 0
        #         index = jump_instr.offset >> 1

        #         # Update the argument of the branching instruction, adding
        #         # EXTENDED_ARGs if needed
        #         while new_arg:
        #             if index and instructions[index - 1].opcode is EXTENDED_ARG:
        #                 index -= 1
        #                 instructions[index].arg = new_arg & 0xFF
        #             else:
        #                 ext_instr = Instruction(index << 1, EXTENDED_ARG, new_arg & 0xFF)
        #                 instructions.insert(index, ext_instr)
        #                 c += 1
        #                 # If the jump instruction was a target of another jump,
        #                 # make the latest EXTENDED_ARG instruction the target
        #                 # of that jump.
        #                 if jump_instr.targets:
        #                     for target in jump_instr.targets:
        #                         if target.end is not jump_instr:
        #                             raise ValueError("Invalid target")
        #                         target.end = ext_instr
        #                     ext_instr.targets.extend(jump_instr.targets)
        #                     jump_instr.targets.clear()
        #             new_arg >>= 8

        #         # Check if we added any EXTENDED_ARGs because we would have to
        #         # reprocess the branches.
        #         # TODO[perf]: only reprocess the branches that are affected.
        #         # However, this branch is not expected to be taken often.
        #         if c:
        #             exts.append((ext_instr, c))
        #             # Update the instruction offset from the point of insertion
        #             # of the EXTENDED_ARGs
        #             for instr_index, instr in enumerate(instructions[index + 1:], index + 1):
        #                 instr.offset = instr_index << 1

        #             process_branches = True

        # Create the new code object
    new_code = bytearray()
    for instr in instructions:
        new_code.append(instr.opcode)
        new_code.append(instr.arg)

    # # Instrument nested code objects recursively
    # for original_offset, nested_code in enumerate(code.co_consts):
    #     if isinstance(nested_code, CodeType):
    #         new_consts[original_offset], nested_lines = instrument_all_lines(nested_code, trap_func, trap_arg, package)
    #         # seen_lines.update(nested_lines)

    return code.replace(
        co_code=bytes(new_code),
        co_stacksize=code.co_stacksize,  # TODO: Compute the value!
        # co_linetable=update_location_data(code, injection_points, [(instr.offset, s) for instr, s in exts]),
        # co_exceptiontable=compile_exception_table(exc_table),
    )


def compile_exception_table(exc_table: t.List[ExceptionTableEntry]) -> bytes:
    table = bytearray()
    for entry in exc_table:
        size = entry.end.offset - entry.start.offset + 2
        table.extend(_to_varint(entry.start.offset >> 1, True))
        table.extend(_to_varint(size >> 1))
        table.extend(_to_varint(entry.target.offset >> 1))
        table.extend(_to_varint(entry.depth_lasti))
    return bytes(table)


def consume_varint(stream: t.Iterable[int]) -> bytes:
    a = bytearray()

    b = next(stream)
    a.append(b)

    value = b & 0x3F
    while b & 0x40:
        b = next(stream)
        a.append(b)

        value = (value << 6) | (b & 0x3F)

    return bytes(a)


consume_signed_varint = consume_varint  # They are the same thing for our purposes


def update_location_data(
    code: CodeType, trap_map: t.Dict[int, int], ext_arg_offsets: t.List[t.Tuple[int, int]]
) -> bytes:
    # DEV: We expect the original offsets in the trap_map
    new_data = bytearray()

    data = code.co_linetable
    data_iter = iter(data)
    ext_arg_offset_iter = iter(sorted(ext_arg_offsets))
    ext_arg_offset, ext_arg_size = next(ext_arg_offset_iter, (None, None))

    original_offset = offset = 0
    while True:
        try:
            chunk = bytearray()

            b = next(data_iter)

            chunk.append(b)

            offset_delta = ((b & 7) + 1) << 1
            loc_code = (b >> 3) & 0xF

            if loc_code == 14:
                chunk.extend(consume_signed_varint(data_iter))
                for _ in range(3):
                    chunk.extend(consume_varint(data_iter))
            elif loc_code == 13:
                chunk.extend(consume_signed_varint(data_iter))
            elif 10 <= loc_code <= 12:
                for _ in range(2):
                    chunk.append(next(data_iter))
            elif 0 <= loc_code <= 9:
                chunk.append(next(data_iter))

            if original_offset in trap_map:
                # No location info for the trap bytecode
                trap_size = trap_map[original_offset]
                n, r = divmod(trap_size, 8)
                for _ in range(n):
                    new_data.append(0x80 | (0xF << 3) | 7)
                if r:
                    new_data.append(0x80 | (0xF << 3) | r - 1)
                offset += trap_size << 1

            # Extend the line table record if we added any EXTENDED_ARGs
            original_offset += offset_delta
            offset += offset_delta
            if ext_arg_offset is not None and offset > ext_arg_offset:
                room = 7 - offset_delta
                chunk[0] += min(room, t.cast(int, ext_arg_size))
                if room < t.cast(int, ext_arg_size):
                    chunk.append(0x80 | (0xF << 3) | t.cast(int, ext_arg_size) - room)
                offset += ext_arg_size << 1

                ext_arg_offset, ext_arg_size = next(ext_arg_offset_iter, (None, None))

            new_data.extend(chunk)
        except StopIteration:
            break

    return bytes(new_data)
