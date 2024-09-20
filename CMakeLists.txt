cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME substrait)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
project(${TARGET_NAME})
include_directories(src/include)

include_directories(third_party/substrait)
include_directories(third_party/)

# refer source by absolute path. So that we can use the same source in the child profile too (i.e. test/c)
set(THIRD_PARTY_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party")
set(PROTOBUF_SOURCES
    ${THIRD_PARTY_DIR}/google/protobuf/any.cc
    ${THIRD_PARTY_DIR}/google/protobuf/any.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/any_lite.cc
    ${THIRD_PARTY_DIR}/google/protobuf/arena.cc
    ${THIRD_PARTY_DIR}/google/protobuf/arenastring.cc
    ${THIRD_PARTY_DIR}/google/protobuf/descriptor.cc
    ${THIRD_PARTY_DIR}/google/protobuf/descriptor.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/descriptor_database.cc
    ${THIRD_PARTY_DIR}/google/protobuf/dynamic_message.cc
    ${THIRD_PARTY_DIR}/google/protobuf/empty.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/extension_set.cc
    ${THIRD_PARTY_DIR}/google/protobuf/extension_set_heavy.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_enum_util.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_message_bases.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_message_reflection.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_message_table_driven.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_message_table_driven_lite.cc
    ${THIRD_PARTY_DIR}/google/protobuf/generated_message_util.cc
    ${THIRD_PARTY_DIR}/google/protobuf/implicit_weak_message.cc
    ${THIRD_PARTY_DIR}/google/protobuf/inlined_string_field.cc
    ${THIRD_PARTY_DIR}/google/protobuf/map.cc
    ${THIRD_PARTY_DIR}/google/protobuf/map_field.cc
    ${THIRD_PARTY_DIR}/google/protobuf/message.cc
    ${THIRD_PARTY_DIR}/google/protobuf/message_lite.cc
    ${THIRD_PARTY_DIR}/google/protobuf/parse_context.cc
    ${THIRD_PARTY_DIR}/google/protobuf/port_def.inc
    ${THIRD_PARTY_DIR}/google/protobuf/port_undef.inc
    ${THIRD_PARTY_DIR}/google/protobuf/reflection_ops.cc
    ${THIRD_PARTY_DIR}/google/protobuf/repeated_field.cc
    ${THIRD_PARTY_DIR}/google/protobuf/repeated_ptr_field.cc
    ${THIRD_PARTY_DIR}/google/protobuf/text_format.cc
    ${THIRD_PARTY_DIR}/google/protobuf/unknown_field_set.cc
    ${THIRD_PARTY_DIR}/google/protobuf/wire_format.cc
    ${THIRD_PARTY_DIR}/google/protobuf/wire_format_lite.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/coded_stream.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/io_win32.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/strtod.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/tokenizer.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/zero_copy_stream.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/zero_copy_stream_impl.cc
    ${THIRD_PARTY_DIR}/google/protobuf/io/zero_copy_stream_impl_lite.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/common.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/int128.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/status.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/stringpiece.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/stringprintf.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/structurally_valid.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/strutil.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/substitute.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/bytestream.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/json_util.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/datapiece.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/default_value_objectwriter.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/error_listener.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/json_escaping.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/json_objectwriter.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/json_stream_parser.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/json_util.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/object_writer.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/proto_writer.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/protostream_objectsource.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/protostream_objectwriter.cc
    ${THIRD_PARTY_DIR}/google/protobuf/source_context.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/statusor.cc
    ${THIRD_PARTY_DIR}/google/protobuf/stubs/time.cc
    ${THIRD_PARTY_DIR}/google/protobuf/type.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/wrappers.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/struct.pb.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/type_info.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/field_mask_utility.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/type_resolver_util.cc
    ${THIRD_PARTY_DIR}/google/protobuf/util/internal/utility.cc)

set(SUBSTRAIT_SOURCES
    ${THIRD_PARTY_DIR}/substrait/substrait/algebra.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/capabilities.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/function.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/parameterized_types.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/plan.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/type.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/type_expressions.pb.cc
    ${THIRD_PARTY_DIR}/substrait/substrait/extensions/extensions.pb.cc)

# refer source by absolute path. So that we can use the same source in the child profile too (i.e. test/c)
set(EXTENSION_SOURCES_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(EXTENSION_SOURCES
    ${EXTENSION_SOURCES_DIR}/to_substrait.cpp
    ${EXTENSION_SOURCES_DIR}/from_substrait.cpp
    ${EXTENSION_SOURCES_DIR}/substrait_extension.cpp
    ${EXTENSION_SOURCES_DIR}/custom_extensions.cpp
    ${EXTENSION_SOURCES_DIR}/custom_extensions_generated.cpp
    ${SUBSTRAIT_SOURCES}
    ${PROTOBUF_SOURCES})

add_library(${EXTENSION_NAME} STATIC ${EXTENSION_SOURCES})

set(PARAMETERS "-warnings")
build_loadable_extension(${TARGET_NAME} ${PARAMETERS} ${EXTENSION_SOURCES})
add_subdirectory(test/c)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
