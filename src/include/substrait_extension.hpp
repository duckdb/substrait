//===----------------------------------------------------------------------===//
//                         DuckDB
//
// substrait-extension.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/optimizer/timer_util.h"

#include <queue>

namespace duckdb {

class SubstraitExtension : public Extension {
public:
	void Load(DuckDB &db) override;
	std::string Name() override;
};

//class QuerySplit {
//public:
//    QuerySplit() {
//        root_rel_test = new substrait::RelRoot();
//        res = new substrait::Rel();
//    };
//    ~QuerySplit() = default;
//
//    bool VisitPlanRel(const substrait::Rel& plan_rel);
//
//public:
//    substrait::RelRoot *root_rel_test;
//    substrait::Rel *res;
//    std::stack<substrait::Rel > subquery;
//};

} // namespace duckdb
