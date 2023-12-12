#include "custom_extensions/custom_extensions.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// 'geometry'  'u!geometry' fixedchar<L2>'  'interval_year' 'interval_day' 'unknown' 'T' "string"
string TransformTypes(const LogicalTypeId &type) {
	switch (type) {
	case LogicalTypeId::DATE:
		return "date";
	case LogicalTypeId::LIST:
		return "List<T>"; // FIXME: gotta template this.
	case LogicalTypeId::VARCHAR:
		return "varchar<L2>"; // FIXME What is L2?
	case LogicalTypeId::ANY:
		return "any1";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "timestamp_tz";
	case LogicalTypeId::TIME:
		return "time";
	case LogicalTypeId::DECIMAL:
		return "decimal<P1,S1>'"; // FIXME: gotta fix precision and scale
	case LogicalTypeId::BOOLEAN:
		return "boolean?"; // FIXME: why this has a question mark?
	case LogicalTypeId::TIMESTAMP:
		return "timestamp";
	case LogicalTypeId::TINYINT:
		return "i8";
	case LogicalTypeId::SMALLINT:
		return "i16";
	case LogicalTypeId::INTEGER:
		return "i32";
	case LogicalTypeId::BIGINT:
		return "i64";
	case LogicalTypeId::FLOAT:
		return "fp32";
	case LogicalTypeId::DOUBLE:
		return "fp64";
	default:
		return "";
	}
}

void SubstraitCustomFunctions::InsertCustomFunction(string name_p, vector<string> types_p, string file_path) {
	auto name = std::move(name_p);
	auto types = std::move(types_p);
	custom_functions[{name, types}] = {{name, types}, std::move(file_path)};
}

string SubstraitFunctionExtensions::Stringfy() {
	return "";
	//    string function_signature = name + ":";
	//        for (auto& type: transformed_types){
	//          function_signature += type + "_";
	//        }
	//        function_signature.pop_back();
}

string GetExtensionURI() {
}

SubstraitFunctionExtensions SubstraitCustomFunctions::Get(const string &name, const vector<LogicalType> &types) {
	vector<string> transformed_types;
	for (auto &type : types) {
		transformed_types.emplace_back(TransformTypes(type.id()));
		if (transformed_types.back().empty()) {
			// If it is empty it means we did not find a yaml extension, we return the function name
			// FIXME: I think that we need to output a DuckDB Extension YAML here
			return {{name, {""}}, "duckdb_extension"};
		}
	}
	SubstraitCustomFunction custom_function {name, {transformed_types}};
	auto it = custom_functions.find(custom_function);
	if (it != custom_functions.end()) {
		// We found it in our substrait custom map, return that
		return it->second;
	}
	// we did not find it, return it as a duckdb extension
	return {{name, {""}}, "duckdb_extension"};
}

} // namespace duckdb