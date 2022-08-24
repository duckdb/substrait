.PHONY: all clean format debug release duckdb_debug duckdb_release pull update

all: release

OSX_BUILD_UNIVERSAL_FLAG=
ifeq (${OSX_BUILD_UNIVERSAL}, 1)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

BUILD_FLAGS=-DEXTENSION_STATIC_BUILD=1 -DBUILD_TPCH_EXTENSION=1 ${OSX_BUILD_UNIVERSAL_FLAG}

ifeq (${BUILD_PYTHON}, 1)
	BUILD_FLAGS:=${EXTENSIONS} -DBUILD_PYTHON=1 -DBUILD_JSON_EXTENSION=1 -DBUILD_FTS_EXTENSION=1 -DBUILD_TPCH_EXTENSION=1 -DBUILD_VISUALIZER_EXTENSION=1 -DBUILD_TPCDS_EXTENSION=1
endif
ifeq (${BUILD_R}, 1)
	BUILD_FLAGS:=${EXTENSIONS} -DBUILD_R=1
endif

pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build

debug: pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=Debug ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORY=../../substrait -B. && \
	cmake --build . --config Debug

release: pull
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo ${BUILD_FLAGS} ../../duckdb/CMakeLists.txt -DEXTERNAL_EXTENSION_DIRECTORY=../../substrait -B. && \
	cmake --build . --config Release

test_release:
	./build/release/test/unittest --test-dir . "[sql]"

test:
	./duckdb/build/debug/test/unittest --test-dir . "[sql]"


format:
	clang-format --sort-includes=0 -style=file -i src/from_substrait.cpp src/to_substrait.cpp src/substrait-extension.cpp
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge