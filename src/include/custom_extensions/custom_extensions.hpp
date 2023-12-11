//===----------------------------------------------------------------------===//
//                         DuckDB
//
// custom_extensions/substrait_custom_extensions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/hash.hpp"
#include <unordered_map>

namespace duckdb {

struct SubstraitCustomFunction {
public:
	SubstraitCustomFunction(string name_p, vector<string> arg_types_p)
	    : name(std::move(name_p)), arg_types(std::move(arg_types_p)) {};
	SubstraitCustomFunction() = default;
	bool operator==(const SubstraitCustomFunction &other) const {
		return name == other.name && arg_types == other.arg_types;
	}
	string name;
	vector<string> arg_types;
};
//! Here we define function extensions
class SubstraitFunctionExtensions {
public:
	SubstraitFunctionExtensions(SubstraitCustomFunction function_p, string extension_path_p)
	    : function(std::move(function_p)), extension_path(std::move(extension_path_p)) {};
	SubstraitFunctionExtensions() = default;
	SubstraitCustomFunction function;
	string extension_path;
};

//! Hash function used in out state machine cache, it hashes and combines all options used to generate a state machine
struct HashSubstraitFunctions {
	size_t operator()(SubstraitCustomFunction const &custom_function) const noexcept {
		// Hash Name
		auto hash_name = Hash(custom_function.name.c_str());
		// Hash Input Types
		auto &i_types = custom_function.arg_types;
		auto hash_type = Hash(i_types[0].c_str());
		for (idx_t i = 1; i < i_types.size(); i++) {
			hash_type = CombineHash(hash_type, Hash(i_types[i].c_str()));
		}
		// Combine name and inputs
		return CombineHash(hash_name, hash_type);
	}
};

class SubstraitCustomFunctions {
public:
	std::unordered_map<SubstraitCustomFunction, SubstraitFunctionExtensions, HashSubstraitFunctions> custom_functions;

	void InsertCustomFunction(string name_p, vector<string> types_p, string file_path) {
		auto name = std::move(name_p);
		auto types = std::move(types_p);
		custom_functions[{name, types}] = {{name, types}, std::move(file_path)};
	}
	void Initialize();
};

} // namespace duckdb