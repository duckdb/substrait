cmake_minimum_required(VERSION 2.8.12)
set(TARGET_NAME substrait)
project(${TARGET_NAME})

set(EXTERNAL_EXTENSION_DIRECTORY ../../substrait/src)

add_subdirectory(duckdb)

