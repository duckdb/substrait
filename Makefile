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


pull:
	git submodule init
	git submodule update --recursive --remote

clean:
	rm -rf build
	rm -rf duckdb/build

duckdb_debug:
	cd duckdb && \
	BUILD_TPCH=1 BUILD_PYTHON=1 make debug

duckdb_release:
	cd duckdb && \
	BUILD_TPCH=1 BUILD_PYTHON=1 make release

debug: pull
	mkdir -p build/debug && \
	cd build/debug && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/debug/src ${OSX_BUILD_UNIVERSAL_FLAG}  ../.. && \
	cmake --build .

release: pull
	mkdir -p build/release && \
	cd build/release && \
	cmake $(GENERATOR) $(FORCE_COLOR) -DCMAKE_BUILD_TYPE=RelWithDebInfo -DDUCKDB_INCLUDE_FOLDER=duckdb/src/include -DDUCKDB_LIBRARY_FOLDER=duckdb/build/release/src ${OSX_BUILD_UNIVERSAL_FLAG} ../.. && \
	cmake --build .

test_release:
	./duckdb/build/release/test/unittest --test-dir . "[sql]"

test:
	./duckdb/build/debug/test/unittest --test-dir . "[sql]"


format:
	clang-format --sort-includes=0 -style=file -i src/from_substrait.cpp src/to_substrait.cpp src/substrait-extension.cpp
	cmake-format -i CMakeLists.txt

update:
	git submodule update --remote --merge