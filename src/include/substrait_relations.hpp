//===----------------------------------------------------------------------===//
//                         DuckDB
//
// substrait_relations
//
//
//===----------------------------------------------------------------------===//

#include "duckdb/main/relation/table_function_relation.hpp"
#include "duckdb/main/relation/table_relation.hpp"
#include "duckdb/main/relation/value_relation.hpp"
#include "duckdb/main/relation/view_relation.hpp"
#include "duckdb/main/relation/limit_relation.hpp"
#include "duckdb/main/relation/projection_relation.hpp"
#include "duckdb/main/relation/setop_relation.hpp"
#include "duckdb/main/relation/aggregate_relation.hpp"
#include "duckdb/main/relation/filter_relation.hpp"
#include "duckdb/main/relation/order_relation.hpp"
#include "duckdb/main/relation/join_relation.hpp"
#include "duckdb/main/relation/cross_product_relation.hpp"
#include "duckdb/main/relation.hpp"

namespace duckdb {

class SubstraitJoinRelation : public JoinRelation {
    using JoinRelation::JoinRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};

class SubstraitCrossProductRelation : public CrossProductRelation {
    using CrossProductRelation::CrossProductRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};

class SubstraitLimitRelation : public LimitRelation {
    using LimitRelation::LimitRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitFilterRelation : public FilterRelation {
    using FilterRelation::FilterRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitProjectionRelation : public ProjectionRelation {
    using ProjectionRelation::ProjectionRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitAggregateRelation : public AggregateRelation {
    using AggregateRelation::AggregateRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitTableRelation : public TableRelation {
    using TableRelation::TableRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitViewRelation : public ViewRelation {
    using ViewRelation::ViewRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitTableFunctionRelation : public TableFunctionRelation {
    using TableFunctionRelation::TableFunctionRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitValueRelation : public ValueRelation {
    using ValueRelation::ValueRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitOrderRelation : public OrderRelation {
    using OrderRelation::OrderRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};


class SubstraitSetOpRelation : public SetOpRelation {
    using SetOpRelation::SetOpRelation;
    void TryBindRelation(vector<ColumnDefinition> &columns) override {
        context.GetContext()->InternalTryBindRelation(*this, columns);
    }
};

}