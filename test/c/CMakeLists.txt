cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME substrait_c_test)
project(${TARGET_NAME})

if (CMAKE_VERSION VERSION_LESS "3.1")
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else ()
  set (CMAKE_CXX_STANDARD 11)
endif ()

include_directories(../../duckdb/src/include)
include_directories(../../duckdb/test/include)
include_directories(../../duckdb/third_party/catch)

# get all source files from test/helpers
file(GLOB TEST_HELPER_SOURCES "../../duckdb/test/helpers/*.cpp")
set(ALL_SOURCES ${EXTENSION_SOURCES} ${TEST_HELPER_SOURCES} test_substrait_c_api.cpp)

# this add_executable is needed to make unit test a target
add_executable(test_substrait test_substrait_c_api.cpp ${ALL_SOURCES})
# add duckdb static library to the test so that duckdb symbols are available
target_link_libraries(test_substrait duckdb_static)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_substrait>
    PARENT_SCOPE)
