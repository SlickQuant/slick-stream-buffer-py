/*
 * Python C++ extension for cross-platform atomic operations used by
 * slick-stream-buffer-py.
 *
 * Wraps C++11 std::atomic<uint64_t> / std::atomic<uint32_t> so that Python
 * and C++ processes sharing a memory segment use the SAME lock-free
 * synchronization primitives (identical memory orderings as
 * slick/stream_buffer.hpp).
 */

#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <atomic>
#include <cstdint>

// ---------------------------------------------------------------------------
// 64-bit operations
// ---------------------------------------------------------------------------

// Python function: atomic_compare_exchange_64(addr, expected, desired)
// Returns: (success, actual_value)
static PyObject* py_atomic_compare_exchange_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long expected, desired;

    if (!PyArg_ParseTuple(args, "KKK", &addr, &expected, &desired)) {
        return NULL;
    }

    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);

    uint64_t expected_val = static_cast<uint64_t>(expected);
    uint64_t desired_val = static_cast<uint64_t>(desired);

    bool success = atomic_ptr->compare_exchange_weak(
        expected_val,
        desired_val,
        std::memory_order_release,  // success order
        std::memory_order_relaxed   // failure order
    );

    return Py_BuildValue("(iK)", success ? 1 : 0, (unsigned long long)expected_val);
}

// Python function: atomic_exchange_64(addr, value)
// Returns: previous_value
static PyObject* py_atomic_exchange_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long value;

    if (!PyArg_ParseTuple(args, "KK", &addr, &value)) {
        return NULL;
    }

    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);
    uint64_t previous = atomic_ptr->exchange(value, std::memory_order_release);

    return PyLong_FromUnsignedLongLong(previous);
}

// Python function: atomic_load_64(addr)
// Returns: value
static PyObject* py_atomic_load_64(PyObject* self, PyObject* args) {
    unsigned long long addr;

    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }

    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);
    uint64_t value = atomic_ptr->load(std::memory_order_acquire);

    return PyLong_FromUnsignedLongLong(value);
}

// Python function: atomic_store_64(addr, value)
// Returns: None
static PyObject* py_atomic_store_64(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned long long value;

    if (!PyArg_ParseTuple(args, "KK", &addr, &value)) {
        return NULL;
    }

    if (addr % 8 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 8-byte aligned for 64-bit atomic operations");
        return NULL;
    }

    std::atomic<uint64_t>* atomic_ptr = reinterpret_cast<std::atomic<uint64_t>*>(addr);
    atomic_ptr->store(value, std::memory_order_release);

    Py_RETURN_NONE;
}

// Alias for atomic_compare_exchange_64 (for compatibility with Linux/macOS naming)
static PyObject* py_atomic_cas_64(PyObject* self, PyObject* args) {
    return py_atomic_compare_exchange_64(self, args);
}

// ---------------------------------------------------------------------------
// 32-bit operations (init_state handshake - must match the C++ side's
// std::atomic<uint32_t> CAS width exactly, stream_buffer.hpp:663-665)
// ---------------------------------------------------------------------------

// Python function: atomic_compare_exchange_32(addr, expected, desired)
// Returns: (success, actual_value)
static PyObject* py_atomic_compare_exchange_32(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned int expected, desired;

    if (!PyArg_ParseTuple(args, "KII", &addr, &expected, &desired)) {
        return NULL;
    }

    if (addr % 4 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 4-byte aligned for 32-bit atomic operations");
        return NULL;
    }

    std::atomic<uint32_t>* atomic_ptr = reinterpret_cast<std::atomic<uint32_t>*>(addr);

    uint32_t expected_val = static_cast<uint32_t>(expected);
    uint32_t desired_val = static_cast<uint32_t>(desired);

    // acq_rel on success matches the C++ creator handshake (stream_buffer.hpp:665)
    bool success = atomic_ptr->compare_exchange_strong(
        expected_val,
        desired_val,
        std::memory_order_acq_rel,  // success order
        std::memory_order_acquire   // failure order
    );

    return Py_BuildValue("(iI)", success ? 1 : 0, (unsigned int)expected_val);
}

// Python function: atomic_load_32(addr)
// Returns: value
static PyObject* py_atomic_load_32(PyObject* self, PyObject* args) {
    unsigned long long addr;

    if (!PyArg_ParseTuple(args, "K", &addr)) {
        return NULL;
    }

    if (addr % 4 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 4-byte aligned for 32-bit atomic operations");
        return NULL;
    }

    std::atomic<uint32_t>* atomic_ptr = reinterpret_cast<std::atomic<uint32_t>*>(addr);
    uint32_t value = atomic_ptr->load(std::memory_order_acquire);

    return PyLong_FromUnsignedLong(value);
}

// Python function: atomic_store_32(addr, value)
// Returns: None
static PyObject* py_atomic_store_32(PyObject* self, PyObject* args) {
    unsigned long long addr;
    unsigned int value;

    if (!PyArg_ParseTuple(args, "KI", &addr, &value)) {
        return NULL;
    }

    if (addr % 4 != 0) {
        PyErr_SetString(PyExc_ValueError, "Address must be 4-byte aligned for 32-bit atomic operations");
        return NULL;
    }

    std::atomic<uint32_t>* atomic_ptr = reinterpret_cast<std::atomic<uint32_t>*>(addr);
    atomic_ptr->store(value, std::memory_order_release);

    Py_RETURN_NONE;
}

// Alias for atomic_compare_exchange_32
static PyObject* py_atomic_cas_32(PyObject* self, PyObject* args) {
    return py_atomic_compare_exchange_32(self, args);
}

// Module method definitions
static PyMethodDef AtomicOpsMethods[] = {
    {"atomic_compare_exchange_64", py_atomic_compare_exchange_64, METH_VARARGS,
     "Atomic 64-bit compare-exchange using C++ std::atomic<uint64_t>\n\n"
     "Args:\n"
     "    addr: Memory address (must be 8-byte aligned)\n"
     "    expected: Expected value\n"
     "    desired: Desired value\n\n"
     "Returns:\n"
     "    (success, actual): Tuple of success flag and actual value\n"},

    {"atomic_cas_64", py_atomic_cas_64, METH_VARARGS,
     "Atomic 64-bit compare-and-swap (alias for atomic_compare_exchange_64)\n"},

    {"atomic_exchange_64", py_atomic_exchange_64, METH_VARARGS,
     "Perform 64-bit atomic exchange (release ordering) using C++11 std::atomic.\n\n"
     "Args:\n"
     "    addr: Memory address (must be 8-byte aligned)\n"
     "    value: New value\n\n"
     "Returns:\n"
     "    previous_value: The value before the exchange\n"},

    {"atomic_load_64", py_atomic_load_64, METH_VARARGS,
     "Perform 64-bit atomic load (acquire ordering) using C++11 std::atomic.\n"},

    {"atomic_store_64", py_atomic_store_64, METH_VARARGS,
     "Perform 64-bit atomic store (release ordering) using C++11 std::atomic.\n"},

    {"atomic_compare_exchange_32", py_atomic_compare_exchange_32, METH_VARARGS,
     "Atomic 32-bit compare-exchange (acq_rel) using C++ std::atomic<uint32_t>\n\n"
     "Args:\n"
     "    addr: Memory address (must be 4-byte aligned)\n"
     "    expected: Expected value\n"
     "    desired: Desired value\n\n"
     "Returns:\n"
     "    (success, actual): Tuple of success flag and actual value\n"},

    {"atomic_cas_32", py_atomic_cas_32, METH_VARARGS,
     "Atomic 32-bit compare-and-swap (alias for atomic_compare_exchange_32)\n"},

    {"atomic_load_32", py_atomic_load_32, METH_VARARGS,
     "Perform 32-bit atomic load (acquire ordering) using C++11 std::atomic.\n"},

    {"atomic_store_32", py_atomic_store_32, METH_VARARGS,
     "Perform 32-bit atomic store (release ordering) using C++11 std::atomic.\n"},

    {NULL, NULL, 0, NULL}
};

// Module definition
static struct PyModuleDef ssb_atomic_ops_ext_module = {
    PyModuleDef_HEAD_INIT,
    "ssb_atomic_ops_ext",
    "Cross-platform atomic operations extension using C++11 std::atomic",
    -1,
    AtomicOpsMethods
};

// Module initialization
PyMODINIT_FUNC PyInit_ssb_atomic_ops_ext(void) {
    return PyModule_Create(&ssb_atomic_ops_ext_module);
}
