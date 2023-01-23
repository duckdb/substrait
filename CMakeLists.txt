cmake_minimum_required(VERSION 2.8.12)

if(DEFINED EXTERNAL_EXTENSION_DIRECTORIES)
  # Extension is being built from DuckDB CMake file, just add the extension dir
  add_subdirectory(substrait)
else()
  set(TARGET_NAME substrait)
  project(${TARGET_NAME})

  # set(EXTENSION_CLASS ${TARGET_NAME}Extension)
  string(SUBSTRING ${TARGET_NAME} 0 1 FIRST_LETTER)
  string(TOUPPER ${FIRST_LETTER} FIRST_LETTER)
  string(REGEX REPLACE "^.(.*)" "${FIRST_LETTER}\\1" EXTENSION_CLASS
                       "${TARGET_NAME}")

  cmake_policy(SET CMP0079 NEW)

  include_directories(${TARGET_NAME}/include)
  add_compile_definitions(DUCKDB_OUT_OF_TREE)
  add_compile_definitions(DUCKDB_EXTENSION_CLASS=${EXTENSION_CLASS}Extension)
  add_compile_definitions(
    DUCKDB_EXTENSION_HEADER="${TARGET_NAME}_extension.hpp")

  set(EXTERNAL_EXTENSION_DIRECTORIES ../${TARGET_NAME})

  add_subdirectory(duckdb)
  add_subdirectory(test/c/)

  target_link_libraries(unittest test_substrait ${TARGET_NAME}_extension)

endif()
