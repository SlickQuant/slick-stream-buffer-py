cmake_minimum_required(VERSION 3.21)
project(slick_stream_buffer_py_interop_tests)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(SLICK_STREAM_BUFFER_PY_BUILD_TESTS "Build tests" ON)

if (SLICK_STREAM_BUFFER_PY_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
