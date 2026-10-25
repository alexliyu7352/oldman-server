cmake_minimum_required(VERSION 3.20)
project(seastar_python_probe LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Python COMPONENTS Interpreter Development.Module REQUIRED)
find_package(pybind11 CONFIG REQUIRED)
find_package(Threads REQUIRED)
# Standalone Asio (header-only), unpacked from dependency-lock.json.
find_path(ASIO_INCLUDE_DIR asio.hpp REQUIRED NO_DEFAULT_PATH
          PATHS ${ASIO_INCLUDE_DIR} ${CMAKE_SOURCE_DIR}/build/deps/sources/asio/include)
add_library(probe_runtime STATIC native/runtime.cc native/server.cc native/http1.cc)
target_include_directories(probe_runtime SYSTEM PUBLIC ${ASIO_INCLUDE_DIR})
target_compile_definitions(probe_runtime PUBLIC ASIO_STANDALONE ASIO_NO_DEPRECATED)
target_link_libraries(probe_runtime PUBLIC Threads::Threads)
# The package's native extension, oldman_server.native._native, built in place.
pybind11_add_module(_native native/bridge.cc)
target_link_libraries(_native PRIVATE probe_runtime ${CMAKE_DL_LIBS})
set_target_properties(_native PROPERTIES LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/src/oldman_server/native)
# The runtime's io_uring build (Asio picks its backend when it is compiled),
# next to the extension, which loads it for NATIVE_REACTOR_BACKEND=io_uring.
find_path(OLDMAN_URING_INCLUDE_DIR liburing.h REQUIRED)
find_library(OLDMAN_URING_LIBRARY NAMES liburing.a REQUIRED)
add_library(oldman_native_uring SHARED native/runtime.cc native/server.cc native/http1.cc)
target_include_directories(oldman_native_uring SYSTEM PRIVATE ${CMAKE_SOURCE_DIR}/native/uring_headers
                           ${ASIO_INCLUDE_DIR} ${OLDMAN_URING_INCLUDE_DIR})
target_compile_definitions(oldman_native_uring PRIVATE OLDMAN_NATIVE_URING ASIO_STANDALONE ASIO_NO_DEPRECATED
                           ASIO_HAS_IO_URING ASIO_DISABLE_EPOLL)
target_link_libraries(oldman_native_uring PRIVATE ${OLDMAN_URING_LIBRARY} Threads::Threads)
set_target_properties(oldman_native_uring PROPERTIES CXX_VISIBILITY_PRESET hidden VISIBILITY_INLINES_HIDDEN ON
                      LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/src/oldman_server/native)
target_link_options(oldman_native_uring PRIVATE -Wl,--exclude-libs,ALL
                    -Wl,--version-script=${CMAKE_SOURCE_DIR}/native/uring_exports.map)
set_property(TARGET oldman_native_uring APPEND PROPERTY LINK_DEPENDS ${CMAKE_SOURCE_DIR}/native/uring_exports.map)
add_dependencies(_native oldman_native_uring)
# Measurement tools of the comparison (its load generator and the native
# reference server), built on Seastar where it is installed.
find_package(Seastar QUIET)
if(Seastar_FOUND)
  add_executable(http_load native/http_load.cc)
  target_link_libraries(http_load PRIVATE Seastar::seastar Threads::Threads)
  target_compile_definitions(http_load PRIVATE SEASTAR_DEFAULT_ALLOCATOR)
  add_executable(sanic_baseline_native probes/sanic_comparison/native.cc)
  target_link_libraries(sanic_baseline_native PRIVATE Seastar::seastar Threads::Threads)
  target_compile_definitions(sanic_baseline_native PRIVATE SEASTAR_DEFAULT_ALLOCATOR)
endif()
