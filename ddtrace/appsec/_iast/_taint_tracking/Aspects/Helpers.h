#pragma once

#include "Initializer/Initializer.h"
#include "TaintTracking/TaintRange.h"
#include <iostream>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <regex>

using namespace pybind11::literals;
namespace py = pybind11;

bool
has_pyerr();

std::string
has_pyerr_as_string();

py::str
has_pyerr_as_pystr();

struct EVIDENCE_MARKS
{
    static constexpr const char* BLANK = "";
    static constexpr const char* START_EVIDENCE = ":+-";
    static constexpr const char* END_EVIDENCE = "-+:";
    static constexpr const char* LESS = "<";
    static constexpr const char* GREATER = ">";
};

inline bool
range_sort(const TaintRangePtr& t1, const TaintRangePtr& t2)
{
    return t1->start < t2->start;
}

inline string
get_tag(const string& content)
{
    if (content.empty()) {
        return { EVIDENCE_MARKS::BLANK };
    }

    return string(EVIDENCE_MARKS::LESS) + content + string(EVIDENCE_MARKS::GREATER);
}

inline string
get_default_content(const TaintRangePtr& taint_range)
{
    if (!taint_range->source.name.empty()) {
        return taint_range->source.name;
    }

    return {};
}

// TODO OPTIMIZATION: check if we can use instead a struct object with range_guid_map, new_ranges and default members so
// we dont have to get the keys by string
/**
 * @brief Replaces a taint range with a new range from the provided dictionary.
 *
 * This function takes a `TaintRangePtr` and an optional dictionary of new ranges.
 * If the `taint_range` is found in the dictionary, it is replaced with the corresponding new range.
 * If the `taint_range` is not found or if `new_ranges` is null, an empty string is returned.
 *
 * @param taint_range A shared pointer to the original taint range.
 * @param new_ranges An optional dictionary containing new taint ranges.
 * @return A string representation of the hash of the new taint range if replaced, otherwise an empty string.
 */
inline string
mapper_replace(const TaintRangePtr& taint_range, const optional<const py::dict>& new_ranges)
{

    if (!taint_range or !new_ranges.has_value() or py::len(new_ranges.value()) == 0) {
        return {};
    }

    const py::dict& new_ranges_value = new_ranges.value();
    py::object o = py::cast(taint_range);

    if (!new_ranges_value.contains(o)) {
        return {};
    }
    const TaintRange new_range = py::cast<TaintRange>((*new_ranges)[o]);
    return to_string(new_range.get_hash());
}

// FIXME: maybe using an "unsigned" -1 as flag is not the best idea...
inline unsigned long int
getNum(const std::string& s)
{
    unsigned long int n = -1;
    try {
        n = std::stoul(s, nullptr, 10);
        if (errno != 0) {
            PyErr_Print();
        }
    } catch (std::exception&) {
        // throw std::invalid_argument("Value is too big");
        PyErr_Print();
    }
    return n;
}

inline PyObject*
process_flag_added_args(PyObject* orig_function, const int flag_added_args, PyObject* args, PyObject* kwargs)
{
    // If orig_function is not None and not the built-in str, bytes, or bytearray, slice args
    if (const auto orig_function_type = Py_TYPE(orig_function);
        orig_function != Py_None && orig_function_type != &PyUnicode_Type && orig_function_type != &PyByteArray_Type &&
        orig_function_type != &PyBytes_Type) {

        if (flag_added_args > 0) {
            const Py_ssize_t num_args = PyTuple_Size(args);
            PyObject* sliced_args = PyTuple_New(num_args - flag_added_args);
            for (Py_ssize_t i = 0; i < num_args - flag_added_args; ++i) {
                // PyTuple_SET_ITEM(sliced_args, i, PyTuple_GET_ITEM(args, i + flag_added_args));
                PyObject* item = PyTuple_GetItem(args, i + flag_added_args);
                Py_INCREF(item);                        // Increase the reference count here
                PyTuple_SET_ITEM(sliced_args, i, item); // Steal the reference
            }
            // Call the original function with the sliced args and return its result
            PyObject* result = PyObject_Call(orig_function, sliced_args, kwargs);
            Py_DECREF(sliced_args);
            return result;
        }
        // Else: call the original function with all args if no slicing is needed
        return PyObject_Call(orig_function, args, kwargs);
    }

    // If orig_function is None or one of the built-in types, just return args for further processing
    // Note: it the caller assigns the PyObject* to a py::object or derivate like with:
    // auto foo py::reinterpret_borrow<py::list>(result_or_args);
    // Then you don't need to Py_INCREF the resulted value. But if it's used as a PyObject*, then you need
    // to do it.
    return args;
}

void
pyexport_aspect_helpers(py::module& m);

// Yup. This is a macro. It's used to wrap the try-catch block around the aspect code to make sure no exceptions
// escape to the user code causing a SIGABRT. This is a common pattern in the IAST codebase.
// Why not a template function? Because performance. Even a simple one like this cause a huge increase in overhead
// on the fastest aspects:

/*
template<typename Func, typename... Args>
auto
exception_wrapper(Func func, const char* aspect_name, Args... args) -> std::optional<decltype(func(args...))>
{
    try {
        return func(args...);
    } catch (const std::exception& e) {
        iast_taint_log_error(std::string(aspect_name) + ": " + e.what());
    } catch (...) {
        iast_taint_log_error(std::string(aspect_name) + ": Unknown error");
    }
    return std::nullopt;
}

The user of this macro could define a parameter (like nullptr) or a "get_results()" function as a lambda, like:

    auto get_result = [&]() -> PyObject* {
        try {
            PyObject* res = do_modulo(candidate_text, candidate_tuple);
            if (res == nullptr) {
                return py_candidate_text.attr("__mod__")(py_candidate_tuple).ptr();
            }
            return res;
        } catch (py::error_already_set& e) {
            e.restore();
            return nullptr;
        }
    };

Please note that you have to handle the error_already_set exception in the lambda, as it's not caught by the macro.

Example calling:
    TRY_CATCH_ASPECT("foo_aspect", return result_o, , {  // no cleanup code here, but the comma is still needed
        // code
    });
*/

#define TRY_CATCH_ASPECT(NAME, RETURNRESULT, CLEANUP, ...)                                                             \
    try {                                                                                                              \
        __VA_ARGS__;                                                                                                   \
    } catch (py::error_already_set & e) {                                                                              \
        e.restore();                                                                                                   \
        CLEANUP;                                                                                                       \
        RETURNRESULT;                                                                                                  \
    } catch (const std::exception& e) {                                                                                \
        const std::string error_message = NAME ". " + std::string(e.what());                                           \
        iast_taint_log_error(error_message);                                                                           \
        CLEANUP;                                                                                                       \
        RETURNRESULT;                                                                                                  \
    } catch (...) {                                                                                                    \
        const std::string error_message = "Unknown IAST propagation error in " NAME ". ";                              \
        iast_taint_log_error(error_message);                                                                           \
        CLEANUP;                                                                                                       \
        RETURNRESULT;                                                                                                  \
    }
