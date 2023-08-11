#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() {
	}
	string query;
	bool enable_optimizer = true;
	bool finished = false;
};

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);
static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized);

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);
static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized);

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	if (input.named_parameters.size() == 1) {
		auto loption = StringUtil::Lower(input.named_parameters.begin()->first);
		if (loption == "enable_optimizer") {
			result->enable_optimizer = BooleanValue::Get(input.named_parameters.begin()->second);
		}
	}
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	return std::move(result);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	if (input.named_parameters.size() == 1) {
		auto loption = StringUtil::Lower(input.named_parameters.begin()->first);
		if (loption == "enable_optimizer") {
			result->enable_optimizer = BooleanValue::Get(input.named_parameters.begin()->second);
		}
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	return std::move(result);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json = false) {
	SubstraitToDuckDB transformer_s2d(conn, serialized, json);
	return transformer_s2d.TransformPlan();
}

static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con,
                                     ToSubstraitFunctionData &data, const string &serialized, bool json) {
	// We round-trip the generated json and verify if the result is the same
	auto actual_result = con.Query(data.query);
	auto sub_relation = SubstraitPlanToDuckDBRel(con, serialized, json);
	auto substrait_result = sub_relation->Execute();
	substrait_result->names = actual_result->names;
	unique_ptr<QueryResult> substrait_materialized;

	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();
		substrait_materialized = stream_query.Materialize();
	} else {
		substrait_materialized = std::move(substrait_result);
	}

	if (!actual_result->Equals(*substrait_materialized)) {
		query_plan->Print();
		sub_relation->Print();
		throw InternalException("The query result of DuckDB's query plan does not match Substrait");
	}
}

static void VerifyBlobRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, false);
}

static void VerifyJSONRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con, ToSubstraitFunctionData &data,
                                const string &serialized) {
	VerifySubstraitRoundtrip(query_plan, con, data, serialized, true);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	// We might want to disable the optimizer of our new connection
	new_conn.context->config.enable_optimizer = data.enable_optimizer;
	new_conn.context->config.use_replacement_scans = false;
	query_plan = new_conn.context->ExtractPlan(data.query);
	DuckDBToSubstrait transformer_d2s(context, *query_plan);
	serialized = transformer_d2s.SerializeToString();

	output.SetValue(0, 0, Value::BLOB_RAW(serialized));
}

static void ToSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToSubFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::VARCHAR});
	ToJsonFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
}

static void ToJsonFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                   Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	// We might want to disable the optimizer of our new connection
	new_conn.context->config.enable_optimizer = data.enable_optimizer;
	new_conn.context->config.use_replacement_scans = false;
	query_plan = new_conn.context->ExtractPlan(data.query);
	DuckDBToSubstrait transformer_d2s(context, *query_plan);
	serialized = transformer_d2s.SerializeToJson();

	output.SetValue(0, 0, serialized);
}

static void ToJsonFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToJsonFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

	if (!context.config.query_verification_enabled) {
		return;
	}
	VerifyJSONRoundtrip(query_plan, new_conn, data, serialized);
	// Also run the ToJson path and verify round-trip for that
	DataChunk other_output;
	other_output.Initialize(context, {LogicalType::BLOB});
	ToSubFunctionInternal(context, data, other_output, new_conn, query_plan, serialized);
	VerifyBlobRoundtrip(query_plan, new_conn, data, serialized);
}

struct FromSubstraitFunctionData : public TableFunctionData {
	FromSubstraitFunctionData() = default;
	shared_ptr<Relation> plan;
	unique_ptr<QueryResult> res;
	unique_ptr<Connection> conn;
};

static unique_ptr<FunctionData> SubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names, bool is_json) {
	auto result = make_uniq<FromSubstraitFunctionData>();
	result->conn = make_uniq<Connection>(*context.db);
	string serialized = input.inputs[0].GetValueUnsafe<string>();
	result->plan = SubstraitPlanToDuckDBRel(*result->conn, serialized, is_json);
	for (auto &column : result->plan->Columns()) {
		return_types.emplace_back(column.Type());
		names.emplace_back(column.Name());
	}
	return std::move(result);
}

static unique_ptr<FunctionData> FromSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, false);
}

static unique_ptr<FunctionData> FromSubstraitBindJSON(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	return SubstraitBind(context, input, return_types, names, true);
}

static void FromSubFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (FromSubstraitFunctionData &)*data_p.bind_data;
	if (!data.res) {
		data.res = data.plan->Execute();
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
	output.Move(*result_chunk);
}

void InitializeGetSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// binary from a valid SQL Query
	TableFunction to_sub_func("get_substrait", {LogicalType::VARCHAR}, ToSubFunction, ToSubstraitBind);
	to_sub_func.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo to_sub_info(to_sub_func);
	catalog.CreateTableFunction(*con.context, to_sub_info);
}

void InitializeGetSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the get_substrait table function that allows us to get a substrait
	// JSON from a valid SQL Query
	TableFunction get_substrait_json("get_substrait_json", {LogicalType::VARCHAR}, ToJsonFunction, ToJsonBind);

	get_substrait_json.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo get_substrait_json_info(get_substrait_json);
	catalog.CreateTableFunction(*con.context, get_substrait_json_info);
}

void InitializeFromSubstrait(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func("from_substrait", {LogicalType::BLOB}, FromSubFunction, FromSubstraitBind);
	CreateTableFunctionInfo from_sub_info(from_sub_func);
	catalog.CreateTableFunction(*con.context, from_sub_info);
}

void InitializeFromSubstraitJSON(Connection &con) {
	auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction from_sub_func_json("from_substrait_json", {LogicalType::VARCHAR}, FromSubFunction,
	                                 FromSubstraitBindJSON);
	CreateTableFunctionInfo from_sub_info_json(from_sub_func_json);
	catalog.CreateTableFunction(*con.context, from_sub_info_json);
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);

	con.Commit();
}

std::string SubstraitExtension::Name() {
	return "substrait";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void substrait_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::SubstraitExtension>();
}

DUCKDB_EXTENSION_API const char *substrait_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}
