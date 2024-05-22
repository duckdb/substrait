#include "from_substrait.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/expression/list.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"

#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/enums/set_operation_type.hpp"

#include "duckdb/parser/expression/comparison_expression.hpp"

#include "substrait/plan.pb.h"
#include "google/protobuf/util/json_util.h"
#include "duckdb/main/client_data.hpp"
#include "duckdb/common/http_state.hpp"

namespace duckdb {
const std::unordered_map<std::string, std::string> SubstraitToDuckDB::function_names_remap = {
    {"modulus", "mod"},      {"std_dev", "stddev"},     {"starts_with", "prefix"},
    {"ends_with", "suffix"}, {"substring", "substr"},   {"char_length", "length"},
    {"is_nan", "isnan"},     {"is_finite", "isfinite"}, {"is_infinite", "isinf"},
    {"like", "~~"},          {"extract", "date_part"}};

const case_insensitive_set_t SubstraitToDuckDB::valid_extract_subfields = {
    "year",    "month",       "day",          "decade", "century", "millenium",
    "quarter", "microsecond", "milliseconds", "second", "minute",  "hour"};

std::string SubstraitToDuckDB::RemapFunctionName(std::string &function_name) {
	// Lets first drop any extension id
	string name;
	for (auto &c : function_name) {
		if (c == ':') {
			break;
		}
		name += c;
	}
	auto it = function_names_remap.find(name);
	if (it != function_names_remap.end()) {
		name = it->second;
	}
	return name;
}

std::string SubstraitToDuckDB::RemoveExtension(std::string &function_name) {
	// Lets first drop any extension id
	string name;
	for (auto &c : function_name) {
		if (c == ':') {
			break;
		}
		name += c;
	}
	return name;
}

SubstraitToDuckDB::SubstraitToDuckDB(Connection &con_p, const string &serialized, bool json) : con(con_p) {
	auto http_state = HTTPState::TryGetState(*con_p.context);
	http_state->Reset();

	if (!json) {
		if (!plan.ParseFromString(serialized)) {
			throw std::runtime_error("Was not possible to convert binary into Substrait plan");
		}
	} else {
		google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(serialized, &plan);
		if (!status.ok()) {
			throw std::runtime_error("Was not possible to convert JSON into Substrait plan: " + status.ToString());
		}
	}

	for (auto &sext : plan.extensions()) {
		if (!sext.has_extension_function()) {
			continue;
		}
		functions_map[sext.extension_function().function_anchor()] = sext.extension_function().name();
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformLiteralExpr(const substrait::Expression &sexpr) {
	const auto &slit = sexpr.literal();
	Value dval;
	if (slit.has_null()) {
		dval = Value(LogicalType::SQLNULL);
		return make_uniq<ConstantExpression>(dval);
	}
	switch (slit.literal_type_case()) {
	case substrait::Expression_Literal::LiteralTypeCase::kFp64:
		dval = Value::DOUBLE(slit.fp64());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kFp32:
		dval = Value::FLOAT(slit.fp32());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kString:
		dval = Value(slit.string());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kDecimal: {
		const auto &substrait_decimal = slit.decimal();
		auto raw_value = (uint64_t *)substrait_decimal.value().c_str();
		hugeint_t substrait_value;
		substrait_value.lower = raw_value[0];
		substrait_value.upper = raw_value[1];
		Value val = Value::HUGEINT(substrait_value);
		auto decimal_type = LogicalType::DECIMAL(substrait_decimal.precision(), substrait_decimal.scale());
		// cast to correct value
		switch (decimal_type.InternalType()) {
		case PhysicalType::INT8:
			dval = Value::DECIMAL(val.GetValue<int8_t>(), substrait_decimal.precision(), substrait_decimal.scale());
			break;
		case PhysicalType::INT16:
			dval = Value::DECIMAL(val.GetValue<int16_t>(), substrait_decimal.precision(), substrait_decimal.scale());
			break;
		case PhysicalType::INT32:
			dval = Value::DECIMAL(val.GetValue<int32_t>(), substrait_decimal.precision(), substrait_decimal.scale());
			break;
		case PhysicalType::INT64:
			dval = Value::DECIMAL(val.GetValue<int64_t>(), substrait_decimal.precision(), substrait_decimal.scale());
			break;
		case PhysicalType::INT128:
			dval = Value::DECIMAL(substrait_value, substrait_decimal.precision(), substrait_decimal.scale());
			break;
		default:
			throw InternalException("Not accepted internal type for decimal");
		}
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kBoolean: {
		dval = Value(slit.boolean());
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kI8:
		dval = Value::TINYINT(slit.i8());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kI32:
		dval = Value::INTEGER(slit.i32());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kI64:
		dval = Value::BIGINT(slit.i64());
		break;
	case substrait::Expression_Literal::LiteralTypeCase::kDate: {
		date_t date(slit.date());
		dval = Value::DATE(date);
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kTime: {
		dtime_t time(slit.time());
		dval = Value::TIME(time);
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kIntervalYearToMonth: {
		interval_t interval;
		interval.months = slit.interval_year_to_month().months();
		interval.days = 0;
		interval.micros = 0;
		dval = Value::INTERVAL(interval);
		break;
	}
	case substrait::Expression_Literal::LiteralTypeCase::kIntervalDayToSecond: {
		interval_t interval;
		interval.months = 0;
		interval.days = slit.interval_day_to_second().days();
		interval.micros = slit.interval_day_to_second().microseconds();
		dval = Value::INTERVAL(interval);
		break;
	}
	default:
		throw InternalException(to_string(slit.literal_type_case()));
	}
	return make_uniq<ConstantExpression>(dval);
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformSelectionExpr(const substrait::Expression &sexpr) {
	if (!sexpr.selection().has_direct_reference() || !sexpr.selection().direct_reference().has_struct_field()) {
		throw InternalException("Can only have direct struct references in selections");
	}
	return make_uniq<PositionalReferenceExpression>(sexpr.selection().direct_reference().struct_field().field() + 1);
}

void SubstraitToDuckDB::VerifyCorrectExtractSubfield(const string &subfield) {
	D_ASSERT(SubstraitToDuckDB::valid_extract_subfields.count(subfield));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformScalarFunctionExpr(const substrait::Expression &sexpr) {
	auto function_name = FindFunction(sexpr.scalar_function().function_reference());
	function_name = RemoveExtension(function_name);
	vector<unique_ptr<ParsedExpression>> children;
	vector<string> enum_expressions;
	auto &function_arguments = sexpr.scalar_function().arguments();
	for (auto &sarg : function_arguments) {
		if (sarg.has_value()) {
			// value expression
			children.push_back(TransformExpr(sarg.value()));
		} else if (sarg.has_type()) {
			// type expression
			throw NotImplementedException("Type arguments in Substrait expressions are not supported yet!");
		} else {
			// enum expression
			D_ASSERT(sarg.has_enum_());
			auto &enum_str = sarg.enum_();
			enum_expressions.push_back(enum_str);
		}
	}
	// string compare galore
	// TODO simplify this
	if (function_name == "and") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_AND, std::move(children));
	} else if (function_name == "or") {
		return make_uniq<ConjunctionExpression>(ExpressionType::CONJUNCTION_OR, std::move(children));
	} else if (function_name == "lt") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHAN, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "equal") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "not_equal") {
		D_ASSERT(children.size() == 2);
		// FIXME: We do a not_like if we are doing a string comparison
		// This is due to substrait not supporting !~~
		bool is_it_string = false;
		for (idx_t child_idx = 0; child_idx < 2; child_idx++) {
			if (children[child_idx]->GetExpressionClass() == ExpressionClass::CONSTANT) {
				auto &constant = children[child_idx]->Cast<ConstantExpression>();
				if (constant.value.type() == LogicalType::VARCHAR) {
					is_it_string = true;
				}
			}
		}
		if (is_it_string) {
			string not_equal = "!~~";
			return make_uniq<FunctionExpression>(not_equal, std::move(children));
		} else {
			return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL, std::move(children[0]),
			                                       std::move(children[1]));
		}

	} else if (function_name == "lte") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "gte") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "gt") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "is_not_null") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, std::move(children[0]));
	} else if (function_name == "is_null") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_IS_NULL, std::move(children[0]));
	} else if (function_name == "not") {
		D_ASSERT(children.size() == 1);
		return make_uniq<OperatorExpression>(ExpressionType::OPERATOR_NOT, std::move(children[0]));
	} else if (function_name == "is_not_distinct_from") {
		D_ASSERT(children.size() == 2);
		return make_uniq<ComparisonExpression>(ExpressionType::COMPARE_NOT_DISTINCT_FROM, std::move(children[0]),
		                                       std::move(children[1]));
	} else if (function_name == "between") {
		// FIXME: ADD between to substrait extension
		D_ASSERT(children.size() == 3);
		return make_uniq<BetweenExpression>(std::move(children[0]), std::move(children[1]), std::move(children[2]));
	} else if (function_name == "extract") {
		D_ASSERT(enum_expressions.size() == 1);
		auto &subfield = enum_expressions[0];
		VerifyCorrectExtractSubfield(subfield);
		auto constant_expression = make_uniq<ConstantExpression>(Value(subfield));
		children.insert(children.begin(), std::move(constant_expression));
	}

	return make_uniq<FunctionExpression>(RemapFunctionName(function_name), std::move(children));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformIfThenExpr(const substrait::Expression &sexpr) {
	const auto &scase = sexpr.if_then();
	auto dcase = make_uniq<CaseExpression>();
	for (const auto &sif : scase.ifs()) {
		CaseCheck dif;
		dif.when_expr = TransformExpr(sif.if_());
		dif.then_expr = TransformExpr(sif.then());
		dcase->case_checks.push_back(std::move(dif));
	}
	dcase->else_expr = TransformExpr(scase.else_());
	return std::move(dcase);
}

LogicalType SubstraitToDuckDB::SubstraitToDuckType(const ::substrait::Type &s_type) {

	if (s_type.has_bool_()) {
		return LogicalType(LogicalTypeId::BOOLEAN);
	} else if (s_type.has_i16()) {
		return LogicalType(LogicalTypeId::SMALLINT);
	} else if (s_type.has_i32()) {
		return LogicalType(LogicalTypeId::INTEGER);
	} else if (s_type.has_decimal()) {
		auto &s_decimal_type = s_type.decimal();
		return LogicalType::DECIMAL(s_decimal_type.precision(), s_decimal_type.scale());
	} else if (s_type.has_i64()) {
		return LogicalType(LogicalTypeId::BIGINT);
	} else if (s_type.has_date()) {
		return LogicalType(LogicalTypeId::DATE);
	} else if (s_type.has_varchar() || s_type.has_string()) {
		return LogicalType(LogicalTypeId::VARCHAR);
	} else if (s_type.has_fp64()) {
		return LogicalType(LogicalTypeId::DOUBLE);
	} else {
		throw InternalException("Substrait type not yet supported");
	}
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformCastExpr(const substrait::Expression &sexpr) {
	const auto &scast = sexpr.cast();
	auto cast_type = SubstraitToDuckType(scast.type());
	auto cast_child = TransformExpr(scast.input());
	return make_uniq<CastExpression>(cast_type, std::move(cast_child));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformInExpr(const substrait::Expression &sexpr) {
	const auto &substrait_in = sexpr.singular_or_list();

	vector<unique_ptr<ParsedExpression>> values;
	values.emplace_back(TransformExpr(substrait_in.value()));

	for (idx_t i = 0; i < (idx_t)substrait_in.options_size(); i++) {
		values.emplace_back(TransformExpr(substrait_in.options(i)));
	}

	return make_uniq<OperatorExpression>(ExpressionType::COMPARE_IN, std::move(values));
}

unique_ptr<ParsedExpression> SubstraitToDuckDB::TransformExpr(const substrait::Expression &sexpr) {
	switch (sexpr.rex_type_case()) {
	case substrait::Expression::RexTypeCase::kLiteral:
		return TransformLiteralExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSelection:
		return TransformSelectionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kScalarFunction:
		return TransformScalarFunctionExpr(sexpr);
	case substrait::Expression::RexTypeCase::kIfThen:
		return TransformIfThenExpr(sexpr);
	case substrait::Expression::RexTypeCase::kCast:
		return TransformCastExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSingularOrList:
		return TransformInExpr(sexpr);
	case substrait::Expression::RexTypeCase::kSubquery:
	default:
		throw InternalException("Unsupported expression type " + to_string(sexpr.rex_type_case()));
	}
}

string SubstraitToDuckDB::FindFunction(uint64_t id) {
	if (functions_map.find(id) == functions_map.end()) {
		throw InternalException("Could not find aggregate function " + to_string(id));
	}
	return functions_map[id];
}

OrderByNode SubstraitToDuckDB::TransformOrder(const substrait::SortField &sordf) {

	OrderType dordertype;
	OrderByNullType dnullorder;

	switch (sordf.direction()) {
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST:
		dordertype = OrderType::ASCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_FIRST;
		break;
	case substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST:
		dordertype = OrderType::DESCENDING;
		dnullorder = OrderByNullType::NULLS_LAST;
		break;
	default:
		throw InternalException("Unsupported ordering " + to_string(sordf.direction()));
	}

	return {dordertype, dnullorder, TransformExpr(sordf.expr())};
}

shared_ptr<Relation> SubstraitToDuckDB::TransformJoinOp(const substrait::Rel &sop) {
	auto &sjoin = sop.join();

	JoinType djointype;
	switch (sjoin.type()) {
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER:
		djointype = JoinType::INNER;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT:
		djointype = JoinType::LEFT;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT:
		djointype = JoinType::RIGHT;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SINGLE:
		djointype = JoinType::SINGLE;
		break;
	case substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_SEMI:
		djointype = JoinType::SEMI;
		break;
	default:
		throw InternalException("Unsupported join type");
	}
	unique_ptr<ParsedExpression> join_condition = TransformExpr(sjoin.expression());
	return make_shared_ptr<JoinRelation>(TransformOp(sjoin.left())->Alias("left"),
	                                     TransformOp(sjoin.right())->Alias("right"), std::move(join_condition),
	                                     djointype);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformCrossProductOp(const substrait::Rel &sop) {
	auto &sub_cross = sop.cross();

	return make_shared_ptr<CrossProductRelation>(TransformOp(sub_cross.left())->Alias("left"),
	                                             TransformOp(sub_cross.right())->Alias("right"));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFetchOp(const substrait::Rel &sop) {
	auto &slimit = sop.fetch();
	return make_shared_ptr<LimitRelation>(TransformOp(slimit.input()), slimit.count(), slimit.offset());
}

shared_ptr<Relation> SubstraitToDuckDB::TransformFilterOp(const substrait::Rel &sop) {
	auto &sfilter = sop.filter();
	return make_shared_ptr<FilterRelation>(TransformOp(sfilter.input()), TransformExpr(sfilter.condition()));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformProjectOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> expressions;
	for (auto &sexpr : sop.project().expressions()) {
		expressions.push_back(TransformExpr(sexpr));
	}

	vector<string> mock_aliases;
	for (size_t i = 0; i < expressions.size(); i++) {
		mock_aliases.push_back("expr_" + to_string(i));
	}
	return make_shared_ptr<ProjectionRelation>(TransformOp(sop.project().input()), std::move(expressions),
	                                           std::move(mock_aliases));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformAggregateOp(const substrait::Rel &sop) {
	vector<unique_ptr<ParsedExpression>> groups, expressions;

	if (sop.aggregate().groupings_size() > 0) {
		for (auto &sgrp : sop.aggregate().groupings()) {
			for (auto &sgrpexpr : sgrp.grouping_expressions()) {
				groups.push_back(TransformExpr(sgrpexpr));
				expressions.push_back(TransformExpr(sgrpexpr));
			}
		}
	}

	for (auto &smeas : sop.aggregate().measures()) {
		vector<unique_ptr<ParsedExpression>> children;
		auto &s_aggr_function = smeas.measure();
		bool is_distinct = s_aggr_function.invocation() ==
		                   substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT;
		for (auto &sarg : s_aggr_function.arguments()) {
			children.push_back(TransformExpr(sarg.value()));
		}
		auto function_name = FindFunction(s_aggr_function.function_reference());
		if (function_name == "count" && children.empty()) {
			function_name = "count_star";
		}
		expressions.push_back(make_uniq<FunctionExpression>(RemapFunctionName(function_name), std::move(children),
		                                                    nullptr, nullptr, is_distinct));
	}

	return make_shared_ptr<AggregateRelation>(TransformOp(sop.aggregate().input()), std::move(expressions),
	                                          std::move(groups));
}

shared_ptr<Relation> SubstraitToDuckDB::TransformReadOp(const substrait::Rel &sop) {
	auto &sget = sop.read();
	shared_ptr<Relation> scan;
	if (sget.has_named_table()) {
		// If we can't find a table with that name, let's try a view.
		try {
			scan = con.Table(sget.named_table().names(0));
		} catch (...) {
			scan = con.View(sget.named_table().names(0));
		}
	} else if (sget.has_local_files()) {
		vector<Value> parquet_files;
		auto local_file_items = sget.local_files().items();
		for (auto &current_file : local_file_items) {
			if (current_file.has_parquet()) {
				if (current_file.has_uri_file()) {
					parquet_files.emplace_back(current_file.uri_file());
				} else if (current_file.has_uri_path()) {
					parquet_files.emplace_back(current_file.uri_path());
				} else {
					throw NotImplementedException("Unsupported type for file path, Only uri_file and uri_path are "
					                              "currently supported");
				}
			} else {
				throw NotImplementedException("Unsupported type of local file for read operator on substrait");
			}
		}
		string name = "parquet_" + StringUtil::GenerateRandomName();
		named_parameter_map_t named_parameters({{"binary_as_string", Value::BOOLEAN(false)}});
		scan = con.TableFunction("parquet_scan", {Value::LIST(parquet_files)}, named_parameters)->Alias(name);
	} else {
		throw NotImplementedException("Unsupported type of read operator for substrait");
	}

	if (sget.has_filter()) {
		scan = make_shared_ptr<FilterRelation>(std::move(scan), TransformExpr(sget.filter()));
	}

	if (sget.has_projection()) {
		vector<unique_ptr<ParsedExpression>> expressions;
		vector<string> aliases;
		idx_t expr_idx = 0;
		for (auto &sproj : sget.projection().select().struct_items()) {
			// FIXME how to get actually alias?
			aliases.push_back("expr_" + to_string(expr_idx++));
			// TODO make sure nothing else is in there
			expressions.push_back(make_uniq<PositionalReferenceExpression>(sproj.field() + 1));
		}

		scan = make_shared_ptr<ProjectionRelation>(std::move(scan), std::move(expressions), std::move(aliases));
	}

	return scan;
}

shared_ptr<Relation> SubstraitToDuckDB::TransformSortOp(const substrait::Rel &sop) {
	vector<OrderByNode> order_nodes;
	for (auto &sordf : sop.sort().sorts()) {
		order_nodes.push_back(TransformOrder(sordf));
	}
	return make_shared_ptr<OrderRelation>(TransformOp(sop.sort().input()), std::move(order_nodes));
}

static duckdb::SetOperationType TransformSetOperationType(substrait::SetRel_SetOp setop) {
	switch (setop) {
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL: {
		return duckdb::SetOperationType::UNION;
	}
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY: {
		return duckdb::SetOperationType::EXCEPT;
	}
	case substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY: {
		return duckdb::SetOperationType::INTERSECT;
	}
	default: {
		throw duckdb::NotImplementedException("SetOperationType transform not implemented for SetRel_SetOp type %d",
		                                      setop);
	}
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformSetOp(const substrait::Rel &sop) {
	D_ASSERT(sop.has_set());
	auto &set = sop.set();
	auto set_op_type = set.op();
	auto type = TransformSetOperationType(set_op_type);

	auto &inputs = set.inputs();
	auto input_count = set.inputs_size();
	if (input_count > 2) {
		throw NotImplementedException("The amount of inputs (%d) is not supported for this set operation", input_count);
	}
	auto lhs = TransformOp(inputs[0]);
	auto rhs = TransformOp(inputs[1]);

	return make_shared_ptr<SetOpRelation>(std::move(lhs), std::move(rhs), type);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformOp(const substrait::Rel &sop) {
	switch (sop.rel_type_case()) {
	case substrait::Rel::RelTypeCase::kJoin:
		return TransformJoinOp(sop);
	case substrait::Rel::RelTypeCase::kCross:
		return TransformCrossProductOp(sop);
	case substrait::Rel::RelTypeCase::kFetch:
		return TransformFetchOp(sop);
	case substrait::Rel::RelTypeCase::kFilter:
		return TransformFilterOp(sop);
	case substrait::Rel::RelTypeCase::kProject:
		return TransformProjectOp(sop);
	case substrait::Rel::RelTypeCase::kAggregate:
		return TransformAggregateOp(sop);
	case substrait::Rel::RelTypeCase::kRead:
		return TransformReadOp(sop);
	case substrait::Rel::RelTypeCase::kSort:
		return TransformSortOp(sop);
	case substrait::Rel::RelTypeCase::kSet:
		return TransformSetOp(sop);
	default:
		throw InternalException("Unsupported relation type " + to_string(sop.rel_type_case()));
	}
}

shared_ptr<Relation> SubstraitToDuckDB::TransformRootOp(const substrait::RelRoot &sop) {
	vector<string> aliases;
	auto column_names = sop.names();
	vector<unique_ptr<ParsedExpression>> expressions;
	int id = 1;
	for (auto &column_name : column_names) {
		aliases.push_back(column_name);
		expressions.push_back(make_uniq<PositionalReferenceExpression>(id++));
	}
	return make_shared_ptr<ProjectionRelation>(TransformOp(sop.input()), std::move(expressions), aliases);
}

shared_ptr<Relation> SubstraitToDuckDB::TransformPlan() {
	if (plan.relations().empty()) {
		throw InvalidInputException("Substrait Plan does not have a SELECT statement");
	}
	auto d_plan = TransformRootOp(plan.relations(0).root());
	return d_plan;
}

} // namespace duckdb
