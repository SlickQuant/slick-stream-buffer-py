include(FetchContent)

# Fetch slick-stream-buffer from GitHub (it pulls slick-shm itself).
#
# For local development without network access, point both dependencies at
# local checkouts when configuring:
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-STREAM-BUFFER=Y:/repo/slick-stream-buffer
#   -DFETCHCONTENT_SOURCE_DIR_SLICK-SHM=Y:/repo/slick-shm
set(BUILD_SLICK_STREAM_BUFFER_TESTS OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(SLICK_SHM_BUILD_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    slick-stream-buffer
    GIT_REPOSITORY https://github.com/SlickQuant/slick-stream-buffer.git
    GIT_TAG v2.1.0
)
FetchContent_MakeAvailable(slick-stream-buffer)

# Find Python
find_package(Python3 COMPONENTS Interpreter REQUIRED)

# Interop test executables built against the REAL C++ header
add_executable(cpp_producer cpp_producer.cpp)
target_link_libraries(cpp_producer PRIVATE slick::stream_buffer)

add_executable(cpp_consumer cpp_consumer.cpp)
target_link_libraries(cpp_consumer PRIVATE slick::stream_buffer)

if(UNIX AND NOT APPLE)
    target_link_libraries(cpp_producer PRIVATE pthread rt)
    target_link_libraries(cpp_consumer PRIVATE pthread rt)
elseif(APPLE)
    target_link_libraries(cpp_producer PRIVATE pthread)
    target_link_libraries(cpp_consumer PRIVATE pthread)
endif()

# Pure-Python test suites
add_test(
    NAME atomic_ops_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_atomic_ops.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME local_mode_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_local_mode.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME shm_mode_test
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_shm_mode.py
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

# Python/C++ interop tests
add_test(
    NAME python_producer_cpp_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test python_producer_cpp_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME cpp_producer_python_consumer
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test cpp_producer_python_consumer
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME cpp_creates_python_reads
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test cpp_creates_python_reads
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME python_attaches_to_cpp_segment_as_creator
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test python_attaches_to_cpp_segment_as_creator
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME wrap_relocation_interop
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test wrap_relocation_interop
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

add_test(
    NAME read_last_interop
    COMMAND ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/tests/test_interop.py --test read_last_interop
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
)

set_tests_properties(
    atomic_ops_test
    local_mode_test
    shm_mode_test
    python_producer_cpp_consumer
    cpp_producer_python_consumer
    cpp_creates_python_reads
    python_attaches_to_cpp_segment_as_creator
    wrap_relocation_interop
    read_last_interop
    PROPERTIES TIMEOUT 120
)
