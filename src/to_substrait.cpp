#include "to_substrait.hpp"

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/function/table/table_scan.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "google/protobuf/util/json_util.h"
#include "substrait/algebra.pb.h"
#include "substrait/plan.pb.h"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/execution/index/art/art_key.hpp"

namespace duckdb {
const std::unordered_map<std::string, std::string> DuckDBToSubstrait::function_names_remap = {
    {"mod", "modulus"},
    {"stddev", "std_dev"},
    {"prefix", "starts_with"},
    {"suffix", "ends_with"},
    {"substr", "substring"},
    {"length", "char_length"},
    {"isnan", "is_nan"},
    {"isfinite", "is_finite"},
    {"isinf", "is_infinite"},
    {"sum_no_overflow", "sum"},
    {"count_star", "count"},
    {"~~", "like"},
    {"*", "multiply"},
    {"-", "subtract"},
    {"+", "add"},
    {"/", "divide"},
    {"first", "any_value"},
    {"!~~", "not_equal"}};

const case_insensitive_set_t DuckDBToSubstrait::valid_extract_subfields = {
    "year",    "month",       "day",          "decade", "century", "millenium",
    "quarter", "microsecond", "milliseconds", "second", "minute",  "hour"};

const SubstraitCustomFunctions DuckDBToSubstrait::custom_functions {};

std::string &DuckDBToSubstrait::RemapFunctionName(std::string &function_name) {
	auto it = function_names_remap.find(function_name);
	if (it != function_names_remap.end()) {
		function_name = it->second;
	}
	return function_name;
}

string DuckDBToSubstrait::SerializeToString() {
	string serialized;
	if (!plan.SerializeToString(&serialized)) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

string DuckDBToSubstrait::SerializeToJson() {
	string serialized;
	auto success = google::protobuf::util::MessageToJsonString(plan, &serialized);
	if (!success.ok()) {
		throw InternalException("It was not possible to serialize the substrait plan");
	}
	return serialized;
}

void DuckDBToSubstrait::AllocateFunctionArgument(substrait::Expression_ScalarFunction *scalar_fun,
                                                 substrait::Expression *value) {
	auto function_argument = new substrait::FunctionArgument();
	function_argument->set_allocated_value(value);
	scalar_fun->mutable_arguments()->AddAllocated(function_argument);
}

string GetRawValue(hugeint_t value) {
	std::string str;
	str.reserve(16);
	auto *byte = (uint8_t *)&value.lower;
	for (idx_t i = 0; i < 8; i++) {
		str.push_back(byte[i]);
	}
	byte = (uint8_t *)&value.upper;
	for (idx_t i = 0; i < 8; i++) {
		str.push_back(byte[i]);
	}

	return str;
}

void DuckDBToSubstrait::TransformDecimal(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	uint8_t scale, width;
	hugeint_t hugeint_value;
	Value mock_value;
	// alright time for some dirty switcharoo
	switch (dval.type().InternalType()) {
	case PhysicalType::INT8: {
		auto internal_value = dval.GetValueUnsafe<int8_t>();
		mock_value = Value::TINYINT(internal_value);
		break;
	}

	case PhysicalType::INT16: {
		auto internal_value = dval.GetValueUnsafe<int16_t>();
		mock_value = Value::SMALLINT(internal_value);
		break;
	}
	case PhysicalType::INT32: {
		auto internal_value = dval.GetValueUnsafe<int32_t>();
		mock_value = Value::INTEGER(internal_value);
		break;
	}
	case PhysicalType::INT64: {
		auto internal_value = dval.GetValueUnsafe<int64_t>();
		mock_value = Value::BIGINT(internal_value);
		break;
	}
	case PhysicalType::INT128: {
		auto internal_value = dval.GetValueUnsafe<hugeint_t>();
		mock_value = Value::HUGEINT(internal_value);
		break;
	}
	default:
		throw InternalException("Not accepted internal type for decimal");
	}
	hugeint_value = mock_value.GetValue<hugeint_t>();
	auto raw_value = GetRawValue(hugeint_value);

	dval.type().GetDecimalProperties(width, scale);

	allocated_decimal->set_scale(scale);
	allocated_decimal->set_precision(width);
	auto *decimal_value = new string();
	*decimal_value = raw_value;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
}

void DuckDBToSubstrait::TransformInteger(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i32(dval.GetValue<int32_t>());
}

void DuckDBToSubstrait::TransformSmallInt(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i16(dval.GetValue<int16_t>());
}

void DuckDBToSubstrait::TransformDouble(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp64(dval.GetValue<double>());
}

void DuckDBToSubstrait::TransformFloat(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_fp32(dval.GetValue<float>());
}

void DuckDBToSubstrait::TransformBigInt(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_i64(dval.GetValue<int64_t>());
}

void DuckDBToSubstrait::TransformDate(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_date(dval.GetValue<date_t>().days);
}

void DuckDBToSubstrait::TransformTime(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_time(dval.GetValue<dtime_t>().micros);
}

void DuckDBToSubstrait::TransformTimestamp(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

void DuckDBToSubstrait::TransformInterval(Value &dval, substrait::Expression &sexpr) {
	// Substrait supports two types of INTERVAL (interval_year and interval_day)
	// whereas DuckDB INTERVAL combines both in one type. Therefore intervals
	// containing both months and days or seconds will lose some data
	// unfortunately. This implementation opts to set the largest interval value.
	auto &sval = *sexpr.mutable_literal();
	auto months = dval.GetValue<interval_t>().months;
	if (months != 0) {
		auto interval_year = make_uniq<substrait::Expression_Literal_IntervalYearToMonth>();
		interval_year->set_months(months);
		sval.set_allocated_interval_year_to_month(interval_year.release());
	} else {
		auto interval_day = make_uniq<substrait::Expression_Literal_IntervalDayToSecond>();
		interval_day->set_days(dval.GetValue<interval_t>().days);
		interval_day->set_microseconds(dval.GetValue<interval_t>().micros);
		sval.set_allocated_interval_day_to_second(interval_day.release());
	}
}

void DuckDBToSubstrait::TransformVarchar(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	string duck_str = dval.GetValue<string>();
	sval.set_string(dval.GetValue<string>());
}

void DuckDBToSubstrait::TransformBoolean(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_boolean(dval.GetValue<bool>());
}

void DuckDBToSubstrait::TransformHugeInt(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	auto *allocated_decimal = new ::substrait::Expression_Literal_Decimal();
	auto hugeint = dval.GetValueUnsafe<hugeint_t>();
	auto raw_value = GetRawValue(hugeint);
	allocated_decimal->set_scale(0);
	allocated_decimal->set_precision(38);

	auto *decimal_value = new string();
	*decimal_value = raw_value;
	allocated_decimal->set_allocated_value(decimal_value);
	sval.set_allocated_decimal(allocated_decimal);
}

void DuckDBToSubstrait::TransformEnum(Value &dval, substrait::Expression &sexpr) {
	auto &sval = *sexpr.mutable_literal();
	sval.set_string(dval.ToString());
}

void DuckDBToSubstrait::TransformConstant(Value &dval, substrait::Expression &sexpr) {
	if (dval.IsNull()) {
		sexpr.mutable_literal()->mutable_null();
		return;
	}
	auto &duckdb_type = dval.type();
	switch (duckdb_type.id()) {
	case LogicalTypeId::DECIMAL:
		TransformDecimal(dval, sexpr);
		break;
	case LogicalTypeId::INTEGER:
		TransformInteger(dval, sexpr);
		break;
	case LogicalTypeId::SMALLINT:
		TransformSmallInt(dval, sexpr);
		break;
	case LogicalTypeId::BIGINT:
		TransformBigInt(dval, sexpr);
		break;
	case LogicalTypeId::HUGEINT:
		TransformHugeInt(dval, sexpr);
		break;
	case LogicalTypeId::DATE:
		TransformDate(dval, sexpr);
		break;
	case LogicalTypeId::TIME:
		TransformTime(dval, sexpr);
		break;
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP:
		TransformTimestamp(dval, sexpr);
		break;
	case LogicalTypeId::INTERVAL:
		TransformInterval(dval, sexpr);
		break;
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
		TransformVarchar(dval, sexpr);
		break;
	case LogicalTypeId::BOOLEAN:
		TransformBoolean(dval, sexpr);
		break;
	case LogicalTypeId::DOUBLE:
		TransformDouble(dval, sexpr);
		break;
	case LogicalTypeId::FLOAT:
		TransformFloat(dval, sexpr);
		break;
	case LogicalTypeId::ENUM:
		TransformEnum(dval, sexpr);
		break;
	default:
		throw NotImplementedException("Consuming a value of type %s is not supported yet", duckdb_type.ToString());
	}
}

void DuckDBToSubstrait::TransformBoundRefExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dref = (BoundReferenceExpression &)dexpr;
	CreateFieldRef(&sexpr, dref.index + col_offset);
}

void DuckDBToSubstrait::TransformCastExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dcast = (BoundCastExpression &)dexpr;
	auto scast = sexpr.mutable_cast();
	TransformExpr(*dcast.child, *scast->mutable_input(), col_offset);
	*scast->mutable_type() = DuckToSubstraitType(dcast.return_type);
}

bool DuckDBToSubstrait::IsExtractFunction(const string &function_name) const {
	return DuckDBToSubstrait::valid_extract_subfields.count(function_name);
}

void DuckDBToSubstrait::TransformFunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                    uint64_t col_offset) {
	auto &dfun = (BoundFunctionExpression &)dexpr;
	auto sfun = sexpr.mutable_scalar_function();

	auto function_name = dfun.function.name;
	if (IsExtractFunction(function_name)) {
		// Change the name to 'extract', and add an Enum argument containing the subfield
		auto subfield = function_name;
		function_name = "extract";
		auto enum_arg = sfun->add_arguments();
		*enum_arg->mutable_enum_() = subfield;
	}
	vector<::substrait::Type> args_types;
	for (auto &darg : dfun.children) {
		auto sarg = sfun->add_arguments();
		TransformExpr(*darg, *sarg->mutable_value(), col_offset);
		args_types.emplace_back(DuckToSubstraitType(darg->return_type));
	}
	sfun->set_function_reference(RegisterFunction(RemapFunctionName(function_name), args_types));

	auto output_type = sfun->mutable_output_type();
	*output_type = DuckToSubstraitType(dfun.return_type);
}

void DuckDBToSubstrait::TransformConstantExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dconst = (BoundConstantExpression &)dexpr;
	TransformConstant(dconst.value, sexpr);
}

void DuckDBToSubstrait::TransformComparisonExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcomp = (BoundComparisonExpression &)dexpr;

	string fname;
	switch (dexpr.type) {
	case ExpressionType::COMPARE_EQUAL:
		fname = "equal";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		fname = "lt";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		fname = "lte";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		fname = "gt";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		fname = "gte";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		fname = "not_equal";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		fname = "is_not_distinct_from";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dcomp.left->return_type));
	args_types.emplace_back(DuckToSubstraitType(dcomp.right->return_type));
	scalar_fun->set_function_reference(RegisterFunction(fname, args_types));
	auto sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.left, *sarg->mutable_value(), 0);
	sarg = scalar_fun->add_arguments();
	TransformExpr(*dcomp.right, *sarg->mutable_value(), 0);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dcomp.return_type);
}

void DuckDBToSubstrait::TransformConjunctionExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                       uint64_t col_offset) {
	auto &dconj = (BoundConjunctionExpression &)dexpr;
	string fname;
	switch (dexpr.type) {
	case ExpressionType::CONJUNCTION_AND:
		fname = "and";
		break;
	case ExpressionType::CONJUNCTION_OR:
		fname = "or";
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}

	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	for (auto &child : dconj.children) {
		auto s_arg = scalar_fun->add_arguments();
		TransformExpr(*child, *s_arg->mutable_value(), col_offset);
		args_types.emplace_back(DuckToSubstraitType(child->return_type));
	}
	scalar_fun->set_function_reference(RegisterFunction(fname, args_types));

	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dconj.return_type);
}

void DuckDBToSubstrait::TransformNotNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                   uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.children[0]->return_type));
	scalar_fun->set_function_reference(RegisterFunction("is_not_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformCaseExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &dcase = (BoundCaseExpression &)dexpr;
	auto scase = sexpr.mutable_if_then();
	for (auto &dcheck : dcase.case_checks) {
		auto sif = scase->mutable_ifs()->Add();
		TransformExpr(*dcheck.when_expr, *sif->mutable_if_());
		auto then_expr = new substrait::Expression();
		TransformExpr(*dcheck.then_expr, *then_expr);
		// Push a Cast
		auto then = sif->mutable_then();
		auto scast = new substrait::Expression_Cast();
		*scast->mutable_type() = DuckToSubstraitType(dcase.return_type);
		scast->set_allocated_input(then_expr);
		then->set_allocated_cast(scast);
	}
	auto else_expr = new substrait::Expression();
	TransformExpr(*dcase.else_expr, *else_expr);
	// Push a Cast
	auto mutable_else = scase->mutable_else_();
	auto scast = new substrait::Expression_Cast();
	*scast->mutable_type() = DuckToSubstraitType(dcase.return_type);
	scast->set_allocated_input(else_expr);
	else_expr = (substrait::Expression *)scast;
	mutable_else->set_allocated_cast(scast);
}

void DuckDBToSubstrait::TransformInExpression(Expression &dexpr, substrait::Expression &sexpr) {
	auto &duck_in_op = (BoundOperatorExpression &)dexpr;
	auto subs_in_op = sexpr.mutable_singular_or_list();

	// Get the expression
	TransformExpr(*duck_in_op.children[0], *subs_in_op->mutable_value());

	// Get the values
	for (idx_t i = 1; i < duck_in_op.children.size(); i++) {
		subs_in_op->add_options();
		TransformExpr(*duck_in_op.children[i], *subs_in_op->mutable_options(i - 1));
	}
}

void DuckDBToSubstrait::TransformIsNullExpression(Expression &dexpr, substrait::Expression &sexpr,
                                                  uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.children[0]->return_type));
	scalar_fun->set_function_reference(RegisterFunction("is_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformNotExpression(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	auto &dop = (BoundOperatorExpression &)dexpr;
	auto scalar_fun = sexpr.mutable_scalar_function();
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(dop.children[0]->return_type));
	scalar_fun->set_function_reference(RegisterFunction("not", args_types));
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dop.children[0], *s_arg->mutable_value(), col_offset);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(dop.return_type);
}

void DuckDBToSubstrait::TransformExpr(Expression &dexpr, substrait::Expression &sexpr, uint64_t col_offset) {
	switch (dexpr.type) {
	case ExpressionType::BOUND_REF:
		TransformBoundRefExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_CAST:
		TransformCastExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::BOUND_FUNCTION:
		TransformFunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::VALUE_CONSTANT:
		TransformConstantExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_EQUAL:
	case ExpressionType::COMPARE_LESSTHAN:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN:
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		TransformComparisonExpression(dexpr, sexpr);
		break;
	case ExpressionType::CONJUNCTION_AND:
	case ExpressionType::CONJUNCTION_OR:
		TransformConjunctionExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_IS_NOT_NULL:
		TransformNotNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::CASE_EXPR:
		TransformCaseExpression(dexpr, sexpr);
		break;
	case ExpressionType::COMPARE_IN:
		TransformInExpression(dexpr, sexpr);
		break;
	case ExpressionType::OPERATOR_IS_NULL:
		TransformIsNullExpression(dexpr, sexpr, col_offset);
		break;
	case ExpressionType::OPERATOR_NOT:
		TransformNotExpression(dexpr, sexpr, col_offset);
		break;
	default:
		throw InternalException(ExpressionTypeToString(dexpr.type));
	}
}

uint64_t DuckDBToSubstrait::RegisterFunction(const string &name, vector<::substrait::Type> &args_types) {
	if (name.empty()) {
		throw InternalException("Missing function name");
	}
	auto function = custom_functions.Get(name, args_types);
	auto substrait_extensions = plan.mutable_extension_uris();
	if (!function.IsNative()) {
		auto extensionURI = function.GetExtensionURI();
		auto it = extension_uri_map.find(extensionURI);
		if (it == extension_uri_map.end()) {
			// We have to add this extension
			extension_uri_map[extensionURI] = last_uri_id;
			auto allocated_string = new string();
			*allocated_string = extensionURI;
			auto uri = new ::substrait::extensions::SimpleExtensionURI();
			uri->set_allocated_uri(allocated_string);
			uri->set_extension_uri_anchor(last_uri_id);
			substrait_extensions->AddAllocated(uri);
			last_uri_id++;
		}
	}
	if (functions_map.find(function.function.GetName()) == functions_map.end()) {
		auto function_id = last_function_id++;
		auto sfun = plan.add_extensions()->mutable_extension_function();
		sfun->set_function_anchor(function_id);
		sfun->set_name(function.function.GetName());
		if (!function.IsNative()) {
			// We only define URI if not native
			sfun->set_extension_uri_reference(extension_uri_map[function.GetExtensionURI()]);
		} else {
			// Function was not found in the yaml files
			sfun->set_extension_uri_reference(0);
			if (strict) {
				// Produce warning message
				std::ostringstream error;
				// Casting Error Message
				error << "Could not find function \"" << function.function.GetName() << "\" with argument types: (";
				auto types = custom_functions.GetTypes(args_types);
				for (idx_t i = 0; i < types.size(); i++) {
					error << "\'" << types[i] << "\'";
					if (i != types.size() - 1) {
						error << ", ";
					}
				}
				error << ")" << std::endl;
				errors += error.str();
			}
		}
		functions_map[function.function.GetName()] = function_id;
	}
	return functions_map[function.function.GetName()];
}

void DuckDBToSubstrait::CreateFieldRef(substrait::Expression *expr, uint64_t col_idx) {
	auto selection = new ::substrait::Expression_FieldReference();
	selection->mutable_direct_reference()->mutable_struct_field()->set_field((int32_t)col_idx);
	auto root_reference = new ::substrait::Expression_FieldReference_RootReference();
	selection->set_allocated_root_reference(root_reference);
	D_ASSERT(selection->root_type_case() == substrait::Expression_FieldReference::RootTypeCase::kRootReference);
	expr->set_allocated_selection(selection);
	D_ASSERT(expr->has_selection());
}

substrait::Expression *DuckDBToSubstrait::TransformIsNotNullFilter(uint64_t col_idx, LogicalType &column_type,
                                                                   TableFilter &dfilter, LogicalType &return_type) {
	auto s_expr = new substrait::Expression();
	auto scalar_fun = s_expr->mutable_scalar_function();
	vector<::substrait::Type> args_types;

	args_types.emplace_back(DuckToSubstraitType(column_type));

	scalar_fun->set_function_reference(RegisterFunction("is_not_null", args_types));
	auto s_arg = scalar_fun->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(return_type);
	return s_expr;
}

substrait::Expression *DuckDBToSubstrait::TransformConjuctionAndFilter(uint64_t col_idx, LogicalType &column_type,
                                                                       TableFilter &dfilter, LogicalType &return_type) {
	auto &conjunction_filter = (ConjunctionAndFilter &)dfilter;
	return CreateConjunction(conjunction_filter.child_filters, [&](unique_ptr<TableFilter> &in) {
		return TransformFilter(col_idx, column_type, *in, return_type);
	});
}

substrait::Expression *DuckDBToSubstrait::TransformConstantComparisonFilter(uint64_t col_idx, LogicalType &column_type,
                                                                            TableFilter &dfilter,
                                                                            LogicalType &return_type) {
	auto s_expr = new substrait::Expression();
	auto s_scalar = s_expr->mutable_scalar_function();
	auto &constant_filter = (ConstantFilter &)dfilter;
	*s_scalar->mutable_output_type() = DuckToSubstraitType(return_type);
	auto s_arg = s_scalar->add_arguments();
	CreateFieldRef(s_arg->mutable_value(), col_idx);
	s_arg = s_scalar->add_arguments();
	TransformConstant(constant_filter.constant, *s_arg->mutable_value());
	uint64_t function_id;
	vector<::substrait::Type> args_types;
	args_types.emplace_back(DuckToSubstraitType(column_type));

	args_types.emplace_back(DuckToSubstraitType(constant_filter.constant.type()));
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		function_id = RegisterFunction("equal", args_types);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		function_id = RegisterFunction("lte", args_types);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		function_id = RegisterFunction("lt", args_types);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		function_id = RegisterFunction("gt", args_types);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		function_id = RegisterFunction("gte", args_types);
		break;
	default:
		throw InternalException(ExpressionTypeToString(constant_filter.comparison_type));
	}
	s_scalar->set_function_reference(function_id);
	return s_expr;
}

substrait::Expression *DuckDBToSubstrait::TransformFilter(uint64_t col_idx, LogicalType &column_type,
                                                          TableFilter &dfilter, LogicalType &return_type) {
	switch (dfilter.filter_type) {
	case TableFilterType::IS_NOT_NULL:
		return TransformIsNotNullFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::CONJUNCTION_AND:
		return TransformConjuctionAndFilter(col_idx, column_type, dfilter, return_type);
	case TableFilterType::CONSTANT_COMPARISON:
		return TransformConstantComparisonFilter(col_idx, column_type, dfilter, return_type);
	default:
		throw InternalException("Unsupported table filter type");
	}
}

substrait::Expression *DuckDBToSubstrait::TransformJoinCond(JoinCondition &dcond, uint64_t left_ncol) {
	auto expr = new substrait::Expression();
	string join_comparision;
	switch (dcond.comparison) {
	case ExpressionType::COMPARE_EQUAL:
		join_comparision = "equal";
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		join_comparision = "gt";
		break;
	case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
		join_comparision = "is_not_distinct_from";
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		join_comparision = "gte";
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		join_comparision = "lte";
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		join_comparision = "lt";
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		join_comparision = "not_equal";
		break;
	default:
		throw InternalException("Unsupported join comparison: " + ExpressionTypeToOperator(dcond.comparison));
	}
	vector<::substrait::Type> args_types;
	auto scalar_fun = expr->mutable_scalar_function();
	auto s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.left, *s_arg->mutable_value());
	args_types.emplace_back(DuckToSubstraitType(dcond.left->return_type));

	s_arg = scalar_fun->add_arguments();
	TransformExpr(*dcond.right, *s_arg->mutable_value(), left_ncol);
	args_types.emplace_back(DuckToSubstraitType(dcond.right->return_type));

	LogicalType bool_type = LogicalType::BOOLEAN;
	*scalar_fun->mutable_output_type() = DuckToSubstraitType(bool_type);
	scalar_fun->set_function_reference(RegisterFunction(join_comparision, args_types));

	return expr;
}

void DuckDBToSubstrait::TransformOrder(BoundOrderByNode &dordf, substrait::SortField &sordf) {
	switch (dordf.type) {
	case OrderType::ASCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_ASC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	case OrderType::DESCENDING:
		switch (dordf.null_order) {
		case OrderByNullType::NULLS_FIRST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_FIRST);
			break;
		case OrderByNullType::NULLS_LAST:
			sordf.set_direction(
			    substrait::SortField_SortDirection::SortField_SortDirection_SORT_DIRECTION_DESC_NULLS_LAST);

			break;
		default:
			throw InternalException("Unsupported ordering type");
		}
		break;
	default:
		throw InternalException("Unsupported ordering type");
	}
	TransformExpr(*dordf.expression, *sordf.mutable_expr());
}

substrait::Rel *DuckDBToSubstrait::TransformFilter(LogicalOperator &dop) {

	auto &dfilter = (LogicalFilter &)dop;

	auto res = TransformOp(*dop.children[0]);

	if (!dfilter.expressions.empty()) {
		auto filter = new substrait::Rel();
		filter->mutable_filter()->set_allocated_input(res);
		filter->mutable_filter()->set_allocated_condition(
		    CreateConjunction(dfilter.expressions, [&](unique_ptr<Expression> &in) {
			    auto expr = new substrait::Expression();
			    TransformExpr(*in, *expr);
			    return expr;
		    }));
		res = filter;
	}

	if (!dfilter.projection_map.empty()) {
		auto projection = new substrait::Rel();
		projection->mutable_project()->set_allocated_input(res);
		for (auto col_idx : dfilter.projection_map) {
			CreateFieldRef(projection->mutable_project()->add_expressions(), col_idx);
		}
		res = projection;
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformProjection(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dproj = (LogicalProjection &)dop;
	auto sproj = res->mutable_project();
	sproj->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dexpr : dproj.expressions) {
		TransformExpr(*dexpr, *sproj->add_expressions());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformTopN(LogicalOperator &dop) {
	auto &dtopn = (LogicalTopN &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();

	auto sord_rel = new substrait::Rel();
	auto sord = sord_rel->mutable_sort();
	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dtopn.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}

	stopn->set_allocated_input(sord_rel);
	stopn->set_offset(dtopn.offset);
	stopn->set_count(dtopn.limit);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformLimit(LogicalOperator &dop) {
	auto &dlimit = (LogicalLimit &)dop;
	auto res = new substrait::Rel();
	auto stopn = res->mutable_fetch();
	stopn->set_allocated_input(TransformOp(*dop.children[0]));

	idx_t limit_val;
	idx_t offset_val;

	switch (dlimit.limit_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		limit_val = dlimit.limit_val.GetConstantValue();
		break;
	case LimitNodeType::UNSET:
		limit_val = 2ULL << 62ULL;
		break;
	default:
		throw InternalException("Unsupported limit value type");
	}
	switch (dlimit.offset_val.Type()) {
	case LimitNodeType::CONSTANT_VALUE:
		offset_val = dlimit.offset_val.GetConstantValue();
		break;
	case LimitNodeType::UNSET:
		offset_val = 0;
		break;
	default:
		throw InternalException("Unsupported offset value type");
	}
	stopn->set_offset(offset_val);
	stopn->set_count(limit_val);
	return res;
}

substrait::Rel *DuckDBToSubstrait::TransformOrderBy(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &dord = (LogicalOrder &)dop;
	auto sord = res->mutable_sort();

	sord->set_allocated_input(TransformOp(*dop.children[0]));

	for (auto &dordf : dord.orders) {
		TransformOrder(dordf, *sord->add_sorts());
	}
	return res;
}

substrait::Rel *DuckDBToSubstrait::ProjectJoinRelation(LogicalComparisonJoin &djoin, substrait::Rel *join_relation,
                                                       idx_t left_col_count) {

	// somewhat odd semantics on our side
	if (djoin.left_projection_map.empty()) {
		for (uint64_t i = 0; i < djoin.children[0]->types.size(); i++) {
			djoin.left_projection_map.push_back(i);
		}
	}
	if (djoin.right_projection_map.empty()) {
		for (uint64_t i = 0; i < djoin.children[1]->types.size(); i++) {
			djoin.right_projection_map.push_back(i);
		}
	}
	auto proj_rel = new substrait::Rel();
	auto projection = proj_rel->mutable_project();
	if (djoin.join_type == JoinType::RIGHT_SEMI || djoin.join_type == JoinType::RIGHT_ANTI) {
		// We project everything from the right table
		for (auto right_idx : djoin.right_projection_map) {
			CreateFieldRef(projection->add_expressions(), right_idx);
		}
	} else {
		for (auto left_idx : djoin.left_projection_map) {
			CreateFieldRef(projection->add_expressions(), left_idx);
		}
		if (djoin.join_type != JoinType::SEMI) {
			for (auto right_idx : djoin.right_projection_map) {
				CreateFieldRef(projection->add_expressions(), right_idx + left_col_count);
			}
		}
	}

	projection->set_allocated_input(join_relation);
	return proj_rel;
}

substrait::Rel *DuckDBToSubstrait::TransformComparisonJoin(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto sjoin = res->mutable_join();
	auto &djoin = (LogicalComparisonJoin &)dop;
	sjoin->set_allocated_left(TransformOp(*dop.children[0]));
	sjoin->set_allocated_right(TransformOp(*dop.children[1]));

	auto left_col_count = dop.children[0]->types.size();
	if (dop.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto child_join = (LogicalComparisonJoin *)dop.children[0].get();
		if (child_join->join_type != JoinType::SEMI && child_join->join_type != JoinType::ANTI) {
			left_col_count = child_join->left_projection_map.size() + child_join->right_projection_map.size();
		} else {
			left_col_count = child_join->left_projection_map.size();
		}
	}
	sjoin->set_allocated_expression(
	    CreateConjunction(djoin.conditions, [&](JoinCondition &in) { return TransformJoinCond(in, left_col_count); }));

	switch (djoin.join_type) {
	case JoinType::INNER:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case JoinType::LEFT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case JoinType::RIGHT:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
	case JoinType::SINGLE:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SINGLE);
		break;
	case JoinType::SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_LEFT_SEMI);
		break;
	case JoinType::MARK:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_MARK);
		break;
	case JoinType::RIGHT_SEMI:
		sjoin->set_type(substrait::JoinRel::JoinType::JoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI);
		break;
	default:
		throw InternalException("Unsupported join type " + JoinTypeToString(djoin.join_type));
	}
	return ProjectJoinRelation(djoin, res, left_col_count);
}

substrait::Rel *DuckDBToSubstrait::TransformDelimiterJoin(LogicalOperator &dop) {
	auto &djoin = dop.Cast<LogicalComparisonJoin>();
	duplicate_eliminated_parent_ptr = &djoin;
	auto res = new substrait::Rel();

	auto sjoin = res->mutable_duplicate_eliminated_join();

	auto lhs_child = TransformOp(*dop.children[0]);
	auto rhs_child = TransformOp(*dop.children[1]);
	if (djoin.delim_flipped) {
		// right side is where our delim is
		sjoin->set_duplicate_eliminated_side(substrait::DuplicateEliminatedJoinRel::DUPLICATE_ELIMINATED_SIDE_RIGHT);
		plan.add_relations()->set_allocated_rel(rhs_child);
		sjoin->set_allocated_left(lhs_child);
		auto rhs_res = new substrait::Rel();
		auto rhs_ref_rel = rhs_res->mutable_reference();
		rhs_ref_rel->set_subtree_ordinal(cur_subtree_relation++);
		sjoin->set_allocated_right(rhs_res);
	} else {
		// left side is where our delim is
		sjoin->set_duplicate_eliminated_side(substrait::DuplicateEliminatedJoinRel::DUPLICATE_ELIMINATED_SIDE_LEFT);
		plan.add_relations()->set_allocated_rel(lhs_child);
		sjoin->set_allocated_right(rhs_child);
		auto lhs_res = new substrait::Rel();
		auto lhs_ref_rel = lhs_res->mutable_reference();
		lhs_ref_rel->set_subtree_ordinal(cur_subtree_relation++);
		sjoin->set_allocated_left(lhs_res);
	}

	auto left_col_count = dop.children[0]->types.size();
	if (dop.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto child_join = (LogicalComparisonJoin *)dop.children[0].get();
		left_col_count = child_join->left_projection_map.size() + child_join->right_projection_map.size();
	}
	sjoin->set_allocated_expression(
	    CreateConjunction(djoin.conditions, [&](JoinCondition &in) { return TransformJoinCond(in, left_col_count); }));

	switch (djoin.join_type) {
	case JoinType::INNER:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_INNER);
		break;
	case JoinType::LEFT:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_LEFT);
		break;
	case JoinType::RIGHT:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_RIGHT);
		break;
	case JoinType::SINGLE:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_LEFT_SINGLE);
		break;
	case JoinType::RIGHT_SEMI:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_RIGHT_SEMI);
		break;
	case JoinType::MARK:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_MARK);
		break;
	case JoinType::RIGHT_ANTI:
		sjoin->set_type(
		    substrait::DuplicateEliminatedJoinRel_JoinType::DuplicateEliminatedJoinRel_JoinType_JOIN_TYPE_RIGHT_ANTI);
		break;
	default:
		throw InternalException("Unsupported join type " + JoinTypeToString(djoin.join_type));
	}

	// Have to add duplicate_eliminated_columns if any
	for (auto &dup_col : djoin.duplicate_eliminated_columns) {
		auto &dref = dup_col->Cast<BoundReferenceExpression>();
		auto s_dup_col = sjoin->add_duplicate_eliminated_columns();
		s_dup_col->mutable_direct_reference()->mutable_struct_field()->set_field(static_cast<int32_t>(dref.index));
	}

	return ProjectJoinRelation(djoin, res, left_col_count);
}

substrait::Rel *DuckDBToSubstrait::TransformAggregateGroup(LogicalOperator &dop) {
	auto res = new substrait::Rel();
	auto &daggr = (LogicalAggregate &)dop;
	auto saggr = res->mutable_aggregate();
	saggr->set_allocated_input(TransformOp(*dop.children[0]));
	// we only do a single grouping set for now
	auto sgrp = saggr->add_groupings();
	for (auto &dgrp : daggr.groups) {
		if (dgrp->type != ExpressionType::BOUND_REF) {
			// TODO push projection or push substrait to allow expressions here
			throw InternalException("No expressions in groupings yet");
		}
		TransformExpr(*dgrp, *sgrp->add_grouping_expressions());
	}
	for (auto &dmeas : daggr.expressions) {
		auto smeas = saggr->add_measures()->mutable_measure();
		if (dmeas->type != ExpressionType::BOUND_AGGREGATE) {
			// TODO push projection or push substrait, too
			throw InternalException("No non-aggregate expressions in measures yet");
		}
		auto &daexpr = (BoundAggregateExpression &)*dmeas;

		*smeas->mutable_output_type() = DuckToSubstraitType(daexpr.return_type);
		vector<::substrait::Type> args_types;
		for (auto &darg : daexpr.children) {
			auto s_arg = smeas->add_arguments();
			args_types.emplace_back(DuckToSubstraitType(darg->return_type));
			TransformExpr(*darg, *s_arg->mutable_value());
		}
		smeas->set_function_reference(RegisterFunction(RemapFunctionName(daexpr.function.name), args_types));
		if (daexpr.aggr_type == AggregateType::DISTINCT) {
			smeas->set_invocation(substrait::AggregateFunction_AggregationInvocation_AGGREGATION_INVOCATION_DISTINCT);
		}
	}
	return res;
}

::substrait::Type DuckDBToSubstrait::DuckToSubstraitType(const LogicalType &type, BaseStatistics *column_statistics,
                                                         bool not_null) {
	::substrait::Type s_type;
	substrait::Type_Nullability type_nullability;
	if (not_null) {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_REQUIRED;
	} else {
		type_nullability = substrait::Type_Nullability::Type_Nullability_NULLABILITY_NULLABLE;
	}
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN: {
		auto bool_type = new substrait::Type_Boolean;
		bool_type->set_nullability(type_nullability);
		s_type.set_allocated_bool_(bool_type);
		return s_type;
	}

	case LogicalTypeId::TINYINT: {
		auto integral_type = new substrait::Type_I8;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i8(integral_type);
		return s_type;
	}
		// Substrait ppl think unsigned types are not common, so we have to upcast
		// these beauties Which completely borks the optimization they are created
		// for
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::SMALLINT: {
		auto integral_type = new substrait::Type_I16;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i16(integral_type);
		return s_type;
	}
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::INTEGER: {
		auto integral_type = new substrait::Type_I32;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i32(integral_type);
		return s_type;
	}
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::BIGINT: {
		auto integral_type = new substrait::Type_I64;
		integral_type->set_nullability(type_nullability);
		s_type.set_allocated_i64(integral_type);
		return s_type;
	}
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT: {
		// FIXME: Support for hugeint types?
		auto s_decimal = new substrait::Type_Decimal();
		s_decimal->set_scale(0);
		s_decimal->set_precision(38);
		s_decimal->set_nullability(type_nullability);
		s_type.set_allocated_decimal(s_decimal);
		return s_type;
	}
	case LogicalTypeId::DATE: {
		auto date_type = new substrait::Type_Date;
		date_type->set_nullability(type_nullability);
		s_type.set_allocated_date(date_type);
		return s_type;
	}
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIME: {
		auto time_type = new substrait::Type_Time;
		time_type->set_nullability(type_nullability);
		s_type.set_allocated_time(time_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_SEC: {
		// FIXME: Shouldn't this have a precision?
		auto timestamp_type = new substrait::Type_Timestamp;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::TIMESTAMP_TZ: {
		auto timestamp_type = new substrait::Type_TimestampTZ;
		timestamp_type->set_nullability(type_nullability);
		s_type.set_allocated_timestamp_tz(timestamp_type);
		return s_type;
	}
	case LogicalTypeId::INTERVAL: {
		auto interval_type = new substrait::Type_IntervalDay();
		interval_type->set_nullability(type_nullability);
		s_type.set_allocated_interval_day(interval_type);
		return s_type;
	}
	case LogicalTypeId::FLOAT: {
		auto float_type = new substrait::Type_FP32;
		float_type->set_nullability(type_nullability);
		s_type.set_allocated_fp32(float_type);
		return s_type;
	}
	case LogicalTypeId::DOUBLE: {
		auto double_type = new substrait::Type_FP64;
		double_type->set_nullability(type_nullability);
		s_type.set_allocated_fp64(double_type);
		return s_type;
	}
	case LogicalTypeId::DECIMAL: {
		auto decimal_type = new substrait::Type_Decimal;
		decimal_type->set_nullability(type_nullability);
		decimal_type->set_precision(DecimalType::GetWidth(type));
		decimal_type->set_scale(DecimalType::GetScale(type));
		s_type.set_allocated_decimal(decimal_type);
		return s_type;
	}
	case LogicalTypeId::VARCHAR: {
		auto string_type = new substrait::Type_String;
		string_type->set_nullability(type_nullability);
		s_type.set_allocated_string(string_type);
		return s_type;
	}
	case LogicalTypeId::BLOB: {
		auto binary_type = new substrait::Type_Binary;
		binary_type->set_nullability(type_nullability);
		s_type.set_allocated_binary(binary_type);
		return s_type;
	}
	case LogicalTypeId::UUID: {
		auto uuid_type = new substrait::Type_UUID;
		uuid_type->set_nullability(type_nullability);
		s_type.set_allocated_uuid(uuid_type);
		return s_type;
	}
	case LogicalTypeId::ENUM: {
		auto enum_type = new substrait::Type_UserDefined;
		enum_type->set_nullability(type_nullability);
		s_type.set_allocated_user_defined(enum_type);
		return s_type;
	}
	default:
		throw NotImplementedException("Logical Type " + type.ToString() +
		                              " not implemented as Substrait Schema Result.");
	}
}

set<idx_t> GetNotNullConstraintCol(TableCatalogEntry &tbl) {
	set<idx_t> not_null;
	for (auto &constraint : tbl.GetConstraints()) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null.insert(((NotNullConstraint *)constraint.get())->index.index);
		}
	}
	return not_null;
}

void DuckDBToSubstrait::TransformTableScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget) {
	auto &table_scan_bind_data = dget.bind_data->Cast<TableScanBindData>();
	auto &table = table_scan_bind_data.table;
	sget->mutable_named_table()->add_names(table.name);
	auto base_schema = new ::substrait::NamedStruct();
	auto type_info = new substrait::Type_Struct();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	auto not_null_constraint = GetNotNullConstraintCol(table);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		if (cur_type.id() == LogicalTypeId::STRUCT) {
			throw std::runtime_error("Structs are not yet accepted in table scans");
		}
		base_schema->add_names(dget.names[i]);
		auto column_statistics = dget.function.statistics(context, &table_scan_bind_data, i);
		bool not_null = not_null_constraint.find(i) != not_null_constraint.end();
		auto new_type = type_info->add_types();
		*new_type = DuckToSubstraitType(cur_type, column_statistics.get(), not_null);
	}
	base_schema->set_allocated_struct_(type_info);
	sget->set_allocated_base_schema(base_schema);
}

void DuckDBToSubstrait::TransformParquetScanToSubstrait(LogicalGet &dget, substrait::ReadRel *sget, BindInfo &bind_info,
                                                        FunctionData &bind_data) {
	auto files_path = bind_info.GetOptionList<string>("file_path");
	for (auto &file_path : files_path) {
		auto parquet_item = sget->mutable_local_files()->add_items();
		// FIXME: should this be uri or file ogw
		auto *path = new string();
		*path = file_path;
		parquet_item->set_allocated_uri_file(path);
		parquet_item->mutable_parquet();
	}

	auto base_schema = new ::substrait::NamedStruct();
	auto type_info = new substrait::Type_Struct();
	type_info->set_nullability(substrait::Type_Nullability_NULLABILITY_REQUIRED);
	for (idx_t i = 0; i < dget.names.size(); i++) {
		auto cur_type = dget.returned_types[i];
		if (cur_type.id() == LogicalTypeId::STRUCT) {
			throw NotImplementedException("Structs are not yet accepted in table scans");
		}
		base_schema->add_names(dget.names[i]);
		auto column_statistics = dget.function.statistics(context, &bind_data, i);
		auto new_type = type_info->add_types();
		*new_type = DuckToSubstraitType(cur_type, column_statistics.get(), false);
	}
	base_schema->set_allocated_struct_(type_info);
	sget->set_allocated_base_schema(base_schema);
}

substrait::Rel *DuckDBToSubstrait::TransformGet(LogicalOperator &dop) {
	auto get_rel = new substrait::Rel();
	substrait::Rel *rel = get_rel;
	auto &dget = (LogicalGet &)dop;

	if (!dget.function.get_bind_info) {
		throw NotImplementedException("This Scanner Type can't be used in substrait because a get batch info "
		                              "is not yet implemented");
	}
	auto bind_info = dget.function.get_bind_info(dget.bind_data.get());
	auto sget = get_rel->mutable_read();

	if (!dget.table_filters.filters.empty()) {
		// Pushdown filter
		auto filter =
		    CreateConjunction(dget.table_filters.filters, [&](std::pair<const idx_t, unique_ptr<TableFilter>> &in) {
			    auto col_idx = in.first;
			    auto return_type = dget.returned_types[col_idx];
			    auto &filter = *in.second;
			    return TransformFilter(col_idx, return_type, filter, return_type);
		    });
		sget->set_allocated_filter(filter);
	}

	if (!dget.projection_ids.empty()) {
		// Projection Pushdown
		auto projection = new substrait::Expression_MaskExpression();
		// fixme: whatever this means
		projection->set_maintain_singular_struct(true);
		auto select = new substrait::Expression_MaskExpression_StructSelect();
		auto &column_ids = dget.GetColumnIds();
		for (auto col_idx : dget.projection_ids) {
			auto struct_item = select->add_struct_items();
			struct_item->set_field(static_cast<int32_t>(column_ids[col_idx]));
			// FIXME do we need to set the child? if yes, to what?
		}
		projection->set_allocated_select(select);
		sget->set_allocated_projection(projection);
	}

	// Add Table Schema
	switch (bind_info.type) {
	case ScanType::TABLE:
		TransformTableScanToSubstrait(dget, sget);
		break;
	case ScanType::PARQUET:
		TransformParquetScanToSubstrait(dget, sget, bind_info, *dget.bind_data);
		break;
	default:
		throw NotImplementedException("This Scan Type is not yet implement for the to_substrait function");
	}

	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformCrossProduct(LogicalOperator &dop) {
	auto rel = new substrait::Rel();
	auto sub_cross_prod = rel->mutable_cross();
	auto &djoin = (LogicalCrossProduct &)dop;
	sub_cross_prod->set_allocated_left(TransformOp(*dop.children[0]));
	sub_cross_prod->set_allocated_right(TransformOp(*dop.children[1]));
	auto bindings = djoin.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformUnion(LogicalOperator &dop) {
	auto rel = new substrait::Rel();

	auto set_op = rel->mutable_set();
	auto &dunion = (LogicalSetOperation &)dop;
	D_ASSERT(dunion.type == LogicalOperatorType::LOGICAL_UNION);

	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_UNION_ALL);
	auto inputs = set_op->mutable_inputs();

	inputs->AddAllocated(TransformOp(*dop.children[0]));
	inputs->AddAllocated(TransformOp(*dop.children[1]));
	auto bindings = dunion.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformDistinct(LogicalOperator &dop) {
	auto rel = new substrait::Rel();

	auto set_op = rel->mutable_set();

	D_ASSERT(dop.children.size() == 1);
	auto &set_operation_p = dop.children[0];

	switch (set_operation_p->type) {
	case LogicalOperatorType::LOGICAL_EXCEPT:
		set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY);
		break;
	case LogicalOperatorType::LOGICAL_INTERSECT:
		set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY);
		break;
	default:
		throw NotImplementedException("Found unexpected child type in Distinct operator");
	}
	auto &set_operation = (LogicalSetOperation &)*set_operation_p;

	auto inputs = set_op->mutable_inputs();

	inputs->AddAllocated(TransformOp(*set_operation.children[0]));
	inputs->AddAllocated(TransformOp(*set_operation.children[1]));
	auto bindings = dop.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformExcept(LogicalOperator &dop) {
	auto rel = new substrait::Rel();
	auto set_op = rel->mutable_set();
	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_MINUS_PRIMARY);
	auto &set_operation = (LogicalSetOperation &)dop;
	auto inputs = set_op->mutable_inputs();
	inputs->AddAllocated(TransformOp(*set_operation.children[0]));
	inputs->AddAllocated(TransformOp(*set_operation.children[1]));
	auto bindings = dop.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformIntersect(LogicalOperator &dop) {
	auto rel = new substrait::Rel();
	auto set_op = rel->mutable_set();
	set_op->set_op(substrait::SetRel_SetOp::SetRel_SetOp_SET_OP_INTERSECTION_PRIMARY);
	auto &set_operation = (LogicalSetOperation &)dop;
	auto inputs = set_op->mutable_inputs();
	inputs->AddAllocated(TransformOp(*set_operation.children[0]));
	inputs->AddAllocated(TransformOp(*set_operation.children[1]));
	auto bindings = dop.GetColumnBindings();
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformDelimGet() {
	auto rel = new substrait::Rel();
	auto delim_get = rel->mutable_duplicate_eliminated_get();
	D_ASSERT(duplicate_eliminated_parent_ptr);
	for (auto &dup_col : duplicate_eliminated_parent_ptr->duplicate_eliminated_columns) {
		auto &dref = dup_col->Cast<BoundReferenceExpression>();
		auto s_dup_col = delim_get->add_column_ids();
		s_dup_col->mutable_direct_reference()->mutable_struct_field()->set_field(static_cast<int32_t>(dref.index));
	}
	auto ref_input = delim_get->mutable_input();
	ref_input->set_subtree_ordinal(cur_subtree_relation);
	duplicate_eliminated_parent_ptr = nullptr;
	return rel;
}

substrait::Rel *DuckDBToSubstrait::TransformOp(LogicalOperator &dop) {
	switch (dop.type) {
	case LogicalOperatorType::LOGICAL_FILTER:
		return TransformFilter(dop);
	case LogicalOperatorType::LOGICAL_TOP_N:
		return TransformTopN(dop);
	case LogicalOperatorType::LOGICAL_LIMIT:
		return TransformLimit(dop);
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return TransformOrderBy(dop);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return TransformProjection(dop);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return TransformDelimiterJoin(dop);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return TransformComparisonJoin(dop);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return TransformAggregateGroup(dop);
	case LogicalOperatorType::LOGICAL_GET:
		return TransformGet(dop);
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return TransformCrossProduct(dop);
	case LogicalOperatorType::LOGICAL_UNION:
		return TransformUnion(dop);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return TransformDistinct(dop);
	case LogicalOperatorType::LOGICAL_EXCEPT:
		return TransformExcept(dop);
	case LogicalOperatorType::LOGICAL_INTERSECT:
		return TransformIntersect(dop);
	case LogicalOperatorType::LOGICAL_DELIM_GET:
		return TransformDelimGet();
	default:
		throw InternalException(LogicalOperatorToString(dop.type));
	}
}

static bool IsSetOperation(LogicalOperator &op) {
	return op.type == LogicalOperatorType::LOGICAL_UNION || op.type == LogicalOperatorType::LOGICAL_EXCEPT ||
	       op.type == LogicalOperatorType::LOGICAL_INTERSECT;
}

void DuckDBToSubstrait::TransformRootOp(substrait::RelRoot &root_rel, LogicalOperator &dop) {
	LogicalOperator *current_op = &dop;
	bool weird_scenario = current_op->type == LogicalOperatorType::LOGICAL_PROJECTION &&
	                      current_op->children[0]->type == LogicalOperatorType::LOGICAL_TOP_N;
	if (weird_scenario) {
		// This is a weird scenario where a projection is put on top of a top-k but
		// the actual aliases are on the projection below the top-k still.
		current_op = current_op->children[0].get();
	}
	// If the root operator is not a projection, we must go down until we find the
	// first projection to get the aliases
	while (current_op->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		if (IsSetOperation(*current_op)) {
			// Take the projection from the first child of the set operation
			D_ASSERT(current_op->children.size() == 2);
			current_op = current_op->children[1].get();
			continue;
		}
		if (current_op->children.size() != 1) {
			throw InternalException("Root node has more than 1, or 0 children (%d) up to "
			                        "reaching a projection node. Type %d",
			                        current_op->children.size(), current_op->type);
		}
		current_op = current_op->children[0].get();
	}
	root_rel.set_allocated_input(TransformOp(dop));
	auto &dproj = (LogicalProjection &)*current_op;
	if (!weird_scenario) {
		for (auto &expression : dproj.expressions) {
			root_rel.add_names(expression->GetName());
		}
	} else {
		for (auto &expression : dop.expressions) {
			D_ASSERT(expression->type == ExpressionType::BOUND_REF);
			auto b_expr = (BoundReferenceExpression *)expression.get();
			root_rel.add_names(dproj.expressions[b_expr->index]->GetName());
		}
	}
}

void DuckDBToSubstrait::TransformPlan(LogicalOperator &dop) {
	auto root = new substrait::RelRoot();
	plan.add_relations()->set_allocated_root(root);
	try {
		TransformRootOp(*root, dop);
	} catch (std::exception &ex) {
		// Ideally we don't have to do that, we should change to capture the error and throw it here at some point
		ErrorData parsed_error(ex);
		throw NotImplementedException("Something in this plan is not yet implemented in the DuckDB -> Substrait plan "
		                              "conversion.\n DuckDB Plan:\n" +
		                              dop.ToString() + "Not Implemented error message: " + parsed_error.RawMessage());
	}
	if (strict && !errors.empty()) {
		throw InvalidInputException("Strict Mode is set to true, and the following warnings/errors happened. \n" +
		                            errors);
	}
	auto version = plan.mutable_version();
	version->set_major_number(0);
	version->set_minor_number(48);
	version->set_patch_number(0);
	auto *producer_name = new string();
	*producer_name = "DuckDB";
	version->set_allocated_producer(producer_name);
}
} // namespace duckdb
