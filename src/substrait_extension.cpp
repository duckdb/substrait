#define DUCKDB_EXTENSION_MAIN

#include "from_substrait.hpp"
#include "substrait_extension.hpp"
#include "to_substrait.hpp"
#include "google/protobuf/util/json_util.h"

#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#endif

namespace duckdb {

struct ToSubstraitFunctionData : public TableFunctionData {
	ToSubstraitFunctionData() {
	}
	string query;
	bool enable_optimizer;
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

static bool SetOptimizationOption(const ClientConfig &config, const duckdb::named_parameter_map_t &named_params) {
	for (const auto &param : named_params) {
		auto loption = StringUtil::Lower(param.first);
		// If the user has explicitly requested to enable/disable the optimizer when
		// generating Substrait, then that takes precedence.
		if (loption == "enable_optimizer") {
			return BooleanValue::Get(param.second);
		}
	}

	// If the user has not specified what they want, fall back to the settings
	// on the connection (e.g. if the optimizer was disabled by the user at
	// the connection level, it would be surprising to enable the optimizer
	// when generating Substrait).
	return config.enable_optimizer;
}

static unique_ptr<ToSubstraitFunctionData> InitToSubstraitFunctionData(const ClientConfig &config,
                                                                       TableFunctionBindInput &input) {
	auto result = make_uniq<ToSubstraitFunctionData>();
	result->query = input.inputs[0].ToString();
	result->enable_optimizer = SetOptimizationOption(config, input.named_parameters);
	return std::move(result);
}

static unique_ptr<FunctionData> ToSubstraitBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BLOB);
	names.emplace_back("Plan Blob");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

static unique_ptr<FunctionData> ToJsonBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("Json");
	auto result = InitToSubstraitFunctionData(context.config, input);
	return std::move(result);
}

shared_ptr<Relation> SubstraitPlanToDuckDBRel(Connection &conn, const string &serialized, bool json = false) {
	SubstraitToDuckDB transformer_s2d(conn, serialized, json);
	return transformer_s2d.TransformPlan();
}

static void VerifySubstraitRoundtrip(unique_ptr<LogicalOperator> &query_plan, Connection &con,
                                     ToSubstraitFunctionData &data, const string &serialized, bool is_json) {
	// We round-trip the generated json and verify if the result is the same
	auto actual_result = con.Query(data.query);

	auto sub_relation = SubstraitPlanToDuckDBRel(con, serialized, is_json);
	auto substrait_result = sub_relation->Execute();
	substrait_result->names = actual_result->names;
	unique_ptr<MaterializedQueryResult> substrait_materialized;

	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();

		substrait_materialized = stream_query.Materialize();
	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
	}
	auto actual_col_coll = actual_result->Collection();
	auto subs_col_coll = substrait_materialized->Collection();
	string error_message;
	if (!ColumnDataCollection::ResultEquals(actual_col_coll, subs_col_coll, error_message)) {
		query_plan->Print();
		sub_relation->Print();
		throw InternalException("The query result of DuckDB's query plan does not match Substrait : " + error_message);
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

static DuckDBToSubstrait InitPlanExtractor(ClientContext &context, ToSubstraitFunctionData &data, Connection &new_conn,
                                           unique_ptr<LogicalOperator> &query_plan) {
	// The user might want to disable the optimizer of the new connection
	new_conn.context->config.enable_optimizer = data.enable_optimizer;
	new_conn.context->config.use_replacement_scans = false;

	// We want for sure to disable the internal compression optimizations.
	// These are DuckDB specific, no other system implements these. Also,
	// respect the user's settings if they chose to disable any specific optimizers.
	//
	// The InClauseRewriter optimization converts large `IN` clauses to a
	// "mark join" against a `ColumnDataCollection`, which may not make
	// sense in other systems and would complicate the conversion to Substrait.
	set<OptimizerType> disabled_optimizers = DBConfig::GetConfig(context).options.disabled_optimizers;
	disabled_optimizers.insert(OptimizerType::IN_CLAUSE);
	disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
    // todo: update filter+read index
    disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
	DBConfig::GetConfig(*new_conn.context).options.disabled_optimizers = disabled_optimizers;

	query_plan = new_conn.context->ExtractPlan(data.query);
	return DuckDBToSubstrait(context, *query_plan);
}

static void ToSubFunctionInternal(ClientContext &context, ToSubstraitFunctionData &data, DataChunk &output,
                                  Connection &new_conn, unique_ptr<LogicalOperator> &query_plan, string &serialized) {
	output.SetCardinality(1);
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
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
	auto transformer_d2s = InitPlanExtractor(context, data, new_conn, query_plan);
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
	if (input.inputs[0].IsNull()) {
		throw BinderException("from_substrait cannot be called with a NULL parameter");
	}
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
    timespec timer = tic();
	if (!data.res) {
		data.res = data.plan->Execute();
	}
	auto result_chunk = data.res->Fetch();
	if (!result_chunk) {
		return;
	}
    toc(&timer, "Duckdb::Relation execution time is\n");
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

std::queue<substrait::Rel > subquery_queue;

void GetSubQueries(substrait::Rel *plan_rel) {
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin:
            GetSubQueries(plan_rel->mutable_join()->mutable_left());
            GetSubQueries(plan_rel->mutable_join()->mutable_right());
            {
                plan_rel->set_split_point();
                subquery_queue.emplace(*plan_rel);
            }
            break;
        case substrait::Rel::RelTypeCase::kCross:
            GetSubQueries(plan_rel->mutable_cross()->mutable_left());
            GetSubQueries(plan_rel->mutable_cross()->mutable_right());
            break;
        case substrait::Rel::RelTypeCase::kFetch:
            GetSubQueries(plan_rel->mutable_fetch()->mutable_input());
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            GetSubQueries(plan_rel->mutable_filter()->mutable_input());
            break;
        case substrait::Rel::RelTypeCase::kProject:
            GetSubQueries(plan_rel->mutable_project()->mutable_input());
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            GetSubQueries(plan_rel->mutable_aggregate()->mutable_input());
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            GetSubQueries(plan_rel->mutable_sort()->mutable_input());
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            GetSubQueries(plan_rel->mutable_set()->mutable_inputs(0));
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }
}

// debug
int debug_split = 1;

bool GetMergedPlan(substrait::Rel *plan_rel, substrait::ReadRel *temp_table) {
    if (plan_rel->split_point) {
//        plan_rel->clear_join();
//        Printer::Print("before plan_rel");
//        Printer::Print(plan_rel.DebugString());
//        // merge with the temp table
//        plan_rel->set_allocated_read(temp_table);
        return true;
    }
    switch (plan_rel->rel_type_case()) {
        case substrait::Rel::RelTypeCase::kJoin:
            if (0 == debug_split) {
//                plan_rel->clear_join();
//                Printer::Print("before plan_rel");
//                Printer::Print(plan_rel->DebugString());
//                // merge with the temp table
//                plan_rel->set_allocated_read(temp_table);
                return true;
            }
            debug_split--;
            GetMergedPlan(plan_rel->mutable_join()->mutable_left(), temp_table);
            GetMergedPlan(plan_rel->mutable_join()->mutable_right(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kCross:
            GetMergedPlan(plan_rel->mutable_cross()->mutable_left(), temp_table);
            GetMergedPlan(plan_rel->mutable_cross()->mutable_right(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kFetch:
            GetMergedPlan(plan_rel->mutable_fetch()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kFilter:
            GetMergedPlan(plan_rel->mutable_filter()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kProject:
            if (GetMergedPlan(plan_rel->mutable_project()->mutable_input(), temp_table)) {
                plan_rel->clear_project();
                Printer::Print("before plan_rel");
                Printer::Print(plan_rel->DebugString());
                // merge with the temp table
                plan_rel->set_allocated_read(temp_table);
            }
            break;
        case substrait::Rel::RelTypeCase::kAggregate:
            GetMergedPlan(plan_rel->mutable_aggregate()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kRead:
            break;
        case substrait::Rel::RelTypeCase::kSort:
            GetMergedPlan(plan_rel->mutable_sort()->mutable_input(), temp_table);
            break;
        case substrait::Rel::RelTypeCase::kSet:
            // todo: fix when meet
            GetMergedPlan(plan_rel->mutable_set()->mutable_inputs(0), temp_table);
            break;
        default:
            throw InternalException("Unsupported relation type " + to_string(plan_rel->rel_type_case()));
    }
    return false;
}

// debug function
void ExecuteSQL(ClientContext &context, Connection &conn, const std::string &sql_command) {
    auto sql_query_plan = conn.context->ExtractPlan(sql_command);
    Printer::Print("sql_query_plan");
    sql_query_plan->Print();
    auto sql_json = DuckDBToSubstrait(context, *sql_query_plan).SerializeToJson();
    // debug
    Printer::Print("sql_json");
    Printer::Print(sql_json);
    auto sql_relation = SubstraitPlanToDuckDBRel(conn, sql_json, true);
    Printer::Print("sql_relation");
    sql_relation->Print();
    auto sql_result = sql_relation->Execute();
}

void PlanTest(ClientContext &context, const std::string &serialized, Connection &new_conn) {
    // parse `serialized` json
    substrait::Plan plan;

    // debug
    Printer::Print("original plan json");
    Printer::Print(serialized);

    google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(serialized, &plan);
    if (!status.ok()) {
        throw std::runtime_error("Was not possible to convert JSON into Substrait plan: " + status.ToString());
    }

    substrait::Rel temp_pointer;
    temp_pointer.CopyFrom(plan.relations(0).root().input());
    GetSubQueries(&temp_pointer);

    substrait::Plan subquery_plan;
    subquery_plan.CopyFrom(plan);

    // debug
    std::queue<std::vector<int32_t>> column_indexes;
    column_indexes.emplace(std::vector<int32_t>{1, 3});
    column_indexes.emplace(std::vector<int32_t>{1, 2, 3});
    std::queue<std::vector<std::string>> expr_names;
    expr_names.emplace(std::vector<std::string>{"movie_id", "keyword"});
    expr_names.emplace(std::vector<std::string>{"id", "title", "movie_id", "keyword"});

    substrait::Plan temp_table_substrait_plan;
//    shared_ptr<Relation> sub_relation;
    unique_ptr<QueryResult> result;

    while (!subquery_queue.empty()) {
        subquery_plan.clear_relations();

        // add projection head
        // add to root_rel_test
        subquery_plan.add_relations()->mutable_root()->mutable_input()->mutable_project()->mutable_input()
            ->CopyFrom(subquery_queue.front());
        // add column indexes
        // todo: get column indexes
        subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->clear_expressions();
        for (const auto &column_index : column_indexes.front()) {
            subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->add_expressions()
                    ->mutable_selection()->mutable_direct_reference()->mutable_struct_field()->set_field(column_index);
        }
        column_indexes.pop();

//    substrait::Expression_FieldReference_RootReference *root_reference = nullptr;
//    if (nullptr == root_reference)
//        root_reference = new ::substrait::Expression_FieldReference_RootReference();
//    subquery_plan.mutable_relations(0)->mutable_root()->mutable_input()->mutable_project()->add_expressions()
//    ->mutable_selection()->set_allocated_root_reference(root_reference);

        // add names for the next subquery
        // todo: get column names
        subquery_plan.mutable_relations(0)->mutable_root()->clear_names();
        for (const auto &expr_name : expr_names.front()) {
            subquery_plan.mutable_relations(0)->mutable_root()->add_names(expr_name);
        }

        Printer::Print("subquery_plan");
        Printer::Print(subquery_plan.DebugString());
        std::string sub_query_str;
        google::protobuf::util::MessageToJsonString(subquery_plan, &sub_query_str);

        // debug
        Printer::Print("subquery_plan json");
        Printer::Print(sub_query_str);

        auto sub_relation = SubstraitPlanToDuckDBRel(new_conn, sub_query_str, true);

        temp_table_substrait_plan.Clear();
        subquery_queue.pop();
        if (subquery_queue.empty()) {
            result = sub_relation->Execute();
            break;
        }

        std::string temp_table_name = "temp_table_" + std::to_string(subquery_queue.size());
        sub_relation->Create(temp_table_name);

        std::string test_select_item;
        for (const auto &expr_name : expr_names.front()) {
            test_select_item.append(temp_table_name).append(".").append(expr_name).append(",");
        }
        // delete the last comma
        test_select_item.pop_back();

        Printer::Print("test_select_item");
        Printer::Print(test_select_item);

        expr_names.pop();

        auto test_select_temp_table = "SELECT " + test_select_item.append(" FROM ").append(temp_table_name).append(";");
        Printer::Print(test_select_temp_table);

        auto temp_table_plan = new_conn.context->ExtractPlan(test_select_temp_table);

        auto temp_plan = DuckDBToSubstrait(context, *temp_table_plan).GetPlan();
        temp_table_substrait_plan.CopyFrom(temp_plan);
        temp_plan.Clear();
        Printer::Print("temp_table_substrait_plan");
        Printer::Print(temp_table_substrait_plan.DebugString());

        GetMergedPlan(&subquery_queue.front(), temp_table_substrait_plan.mutable_relations(0)->mutable_root()
            ->mutable_input()->mutable_project()->mutable_input()->mutable_read());
        Printer::Print("after GetMergedPlan");
        Printer::Print(subquery_queue.front().DebugString());
    }

//    auto result = sub_relation->Execute();
    Printer::Print("result");
    result->Print();

    subquery_plan.Clear();
    plan.Clear();
    temp_table_substrait_plan.Clear();
}

void RelationTest(const shared_ptr<Relation>& relation) {
    // todo: split the `relation`
    auto &proj_rel = relation->Cast<ProjectionRelation>();
    proj_rel.VisitChildren();
    auto child_rel = proj_rel.ChildRelation();
    unique_ptr<QueryResult> debug_res;
    Relation *sub_rel = relation.get();
    while (child_rel) {
        if (child_rel->type == RelationType::JOIN_RELATION) {
            auto &join_rel = child_rel->Cast<JoinRelation>();

            // debug
            auto view = join_rel.CreateView("debug_view");
            auto sub_result = view->Execute();

//            auto table_rel = join_rel.CreateRel(INVALID_SCHEMA, "debug_table");
            break;
        } else {
            sub_rel = child_rel;
            child_rel = child_rel->ChildRelation();
        }
    }

//	auto substrait_result = relation->Execute();
//    // debug
//    unique_ptr<DataChunk> result_chunk;
//    ErrorData error;
//    if (substrait_result->TryFetch(result_chunk, error)) {
//        Printer::Print("substrait_result");
//        result_chunk->Print();
//    }
//
//	unique_ptr<MaterializedQueryResult> substrait_materialized;
//
//	if (substrait_result->type == QueryResultType::STREAM_RESULT) {
//		auto &stream_query = substrait_result->Cast<duckdb::StreamQueryResult>();
//
//		substrait_materialized = stream_query.Materialize();
//	} else if (substrait_result->type == QueryResultType::MATERIALIZED_RESULT) {
//		substrait_materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(substrait_result));
//	}
//	auto subs_col_coll = substrait_materialized->Collection();
//    // debug
//    Printer::Print("subs_col_coll");
//    subs_col_coll.Print();
//
//    // todo: merge the previous result
//    // Create(const string &table_name);
//    // CreateView(const string &name, bool replace = true, bool temporary = false);
}

static void QuerySplit(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    // from `ToJsonFunction`
    auto &data = (ToSubstraitFunctionData &)*data_p.bind_data;
	if (data.finished) {
		return;
	}
	auto new_conn = Connection(*context.db);

	unique_ptr<LogicalOperator> query_plan;
	string serialized;
	ToJsonFunctionInternal(context, data, output, new_conn, query_plan, serialized);

	data.finished = true;

    // execute it
    PlanTest(context, serialized, new_conn);
//    RelationTest(SubstraitPlanToDuckDBRel(new_conn, serialized, false));
}

void InitializeQuerySplit(Connection &con) {
    auto &catalog = Catalog::GetSystemCatalog(*con.context);

	// create the from_substrait table function that allows us to get a query
	// result from a substrait plan
	TableFunction query_split("query_split", {LogicalType::VARCHAR}, QuerySplit, ToJsonBind);

	query_split.named_parameters["enable_optimizer"] = LogicalType::BOOLEAN;
	CreateTableFunctionInfo query_split_info(query_split);
	catalog.CreateTableFunction(*con.context, query_split_info);
}

void SubstraitExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	InitializeGetSubstrait(con);
	InitializeGetSubstraitJSON(con);

	InitializeFromSubstrait(con);
	InitializeFromSubstraitJSON(con);

    InitializeQuerySplit(con);

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
