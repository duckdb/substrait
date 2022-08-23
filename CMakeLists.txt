cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME substrait)
project(${TARGET_NAME})

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0015)
  cmake_policy(SET CMP0015 NEW)
endif()

if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

if(CMAKE_VERSION VERSION_LESS "3.1")
  add_compile_options(-std=c++11)
else()
  set(CMAKE_CXX_STANDARD 11)
endif()

option(
  OSX_BUILD_UNIVERSAL
  "Build both architectures on OSX and create a single binary containing both."
  FALSE)
if(OSX_BUILD_UNIVERSAL)
  if(NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  set(CMAKE_OSX_ARCHITECTURES
      "x86_64;arm64"
      CACHE STRING "Build architectures for Mac OS X" FORCE)
endif()

include_directories(${DUCKDB_INCLUDE_FOLDER})

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_compile_options(-Wall -Werror -pedantic -fsanitize=address)
endif()

link_directories(${DUCKDB_LIBRARY_FOLDER})

include_directories(src/include)

include_directories(include third_party/substrait)
include_directories(include third_party/)

set(PROTOBUF_SOURCES
    third_party/google/protobuf/any.cc
    third_party/google/protobuf/any.pb.cc
    third_party/google/protobuf/any_lite.cc
    third_party/google/protobuf/arena.cc
    third_party/google/protobuf/arenastring.cc
    third_party/google/protobuf/descriptor.cc
    third_party/google/protobuf/descriptor.pb.cc
    third_party/google/protobuf/descriptor_database.cc
    third_party/google/protobuf/dynamic_message.cc
    third_party/google/protobuf/empty.pb.cc
    third_party/google/protobuf/extension_set.cc
    third_party/google/protobuf/extension_set_heavy.cc
    third_party/google/protobuf/generated_enum_util.cc
    third_party/google/protobuf/generated_message_bases.cc
    third_party/google/protobuf/generated_message_reflection.cc
    third_party/google/protobuf/generated_message_table_driven.cc
    third_party/google/protobuf/generated_message_table_driven_lite.cc
    third_party/google/protobuf/generated_message_util.cc
    third_party/google/protobuf/implicit_weak_message.cc
    third_party/google/protobuf/inlined_string_field.cc
    third_party/google/protobuf/map.cc
    third_party/google/protobuf/map_field.cc
    third_party/google/protobuf/message.cc
    third_party/google/protobuf/message_lite.cc
    third_party/google/protobuf/parse_context.cc
    third_party/google/protobuf/port_def.inc
    third_party/google/protobuf/port_undef.inc
    third_party/google/protobuf/reflection_ops.cc
    third_party/google/protobuf/repeated_field.cc
    third_party/google/protobuf/repeated_ptr_field.cc
    third_party/google/protobuf/text_format.cc
    third_party/google/protobuf/unknown_field_set.cc
    third_party/google/protobuf/wire_format.cc
    third_party/google/protobuf/wire_format_lite.cc
    third_party/google/protobuf/io/coded_stream.cc
    third_party/google/protobuf/io/io_win32.cc
    third_party/google/protobuf/io/strtod.cc
    third_party/google/protobuf/io/tokenizer.cc
    third_party/google/protobuf/io/zero_copy_stream.cc
    third_party/google/protobuf/io/zero_copy_stream_impl.cc
    third_party/google/protobuf/io/zero_copy_stream_impl_lite.cc
    third_party/google/protobuf/stubs/common.cc
    third_party/google/protobuf/stubs/int128.cc
    third_party/google/protobuf/stubs/status.cc
    third_party/google/protobuf/stubs/stringpiece.cc
    third_party/google/protobuf/stubs/stringprintf.cc
    third_party/google/protobuf/stubs/structurally_valid.cc
    third_party/google/protobuf/stubs/strutil.cc
    third_party/google/protobuf/stubs/substitute.cc
    third_party/google/protobuf/stubs/bytestream.cc
    third_party/google/protobuf/util/json_util.cc
    third_party/google/protobuf/util/internal/datapiece.cc
    third_party/google/protobuf/util/internal/default_value_objectwriter.cc
    third_party/google/protobuf/util/internal/error_listener.cc
    third_party/google/protobuf/util/internal/json_escaping.cc
    third_party/google/protobuf/util/internal/json_objectwriter.cc
    third_party/google/protobuf/util/internal/json_stream_parser.cc
    third_party/google/protobuf/util/json_util.cc
    third_party/google/protobuf/util/internal/object_writer.cc
    third_party/google/protobuf/util/internal/proto_writer.cc
    third_party/google/protobuf/util/internal/protostream_objectsource.cc
    third_party/google/protobuf/util/internal/protostream_objectwriter.cc
    third_party/google/protobuf/source_context.pb.cc
    third_party/google/protobuf/stubs/statusor.cc
    third_party/google/protobuf/stubs/time.cc
    third_party/google/protobuf/type.pb.cc
    third_party/google/protobuf/wrappers.pb.cc
    third_party/google/protobuf/struct.pb.cc
    third_party/google/protobuf/util/internal/type_info.cc
    third_party/google/protobuf/util/internal/field_mask_utility.cc
    third_party/google/protobuf/util/type_resolver_util.cc
    third_party/google/protobuf/util/internal/utility.cc)

set(SUBSTRAIT_SOURCES
    third_party/substrait/substrait/algebra.pb.cc
    third_party/substrait/substrait/capabilities.pb.cc
    third_party/substrait/substrait/function.pb.cc
    third_party/substrait/substrait/parameterized_types.pb.cc
    third_party/substrait/substrait/plan.pb.cc
    third_party/substrait/substrait/type.pb.cc
    third_party/substrait/substrait/type_expressions.pb.cc
    third_party/substrait/substrait/extensions/extensions.pb.cc)

set(ALL_SOURCES
    src/to_substrait.cpp src/from_substrait.cpp src/substrait-extension.cpp
    ${SUBSTRAIT_SOURCES} ${PROTOBUF_SOURCES})

add_library(${TARGET_NAME} SHARED ${ALL_SOURCES})
set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")

# Ignore warnings
if(MSVC)
  target_compile_options(${TARGET_NAME} PRIVATE "/W0")
elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}"
                                                      STREQUAL "GNU")
  target_compile_options(${TARGET_NAME} PRIVATE "-w")
endif()

if("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  target_link_libraries(${TARGET_NAME} -fsanitize=address)
endif()

set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension")

if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS
                                                  "-undefined dynamic_lookup")
endif()

if(WIN32)
  target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
endif()

if(MSVC)
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
  set_target_properties(
    ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
                              "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
endif()

if(WIN32)
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_DEBUG
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_DEBUG
      "${CMAKE_EXE_LINKER_FLAGS_DEBUG}  /DELAYLOAD:duckdb.dll")
  set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS_RELEASE
                                                  "/DELAYLOAD:duckdb.dll")
  set(CMAKE_EXE_LINKER_FLAGS_RELEASE
      "${CMAKE_EXE_LINKER_FLAGS_RELEASE}  /DELAYLOAD:duckdb.dll")
  # This is only strictly required in non-Visual-Studio builds like Ninja:
  target_link_libraries(${TARGET_NAME} delayimp)
endif()
