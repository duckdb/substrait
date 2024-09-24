import duckdb
import pytest

plan_pb2 = pytest.importorskip("substrait.gen.proto.plan_pb2")
algebra_pb2 = pytest.importorskip("substrait.gen.proto.algebra_pb2")
type_pb2 = pytest.importorskip("substrait.gen.proto.type_pb2")
pa = pytest.importorskip("pyarrow")


def execute_plan(connection: duckdb.DuckDBPyConnection, plan: plan_pb2.Plan) -> pa.lib.Table:
    plan_data = plan.SerializeToString()
    try:
        query_result = connection.from_substrait(proto=plan_data)
    except Exception as err:
        raise ValueError(f'DuckDB Execution Error: {err}') from err
    return query_result.arrow()

def execute_query(connection, table_name: str):
    plan = plan_pb2.Plan(relations=[
        plan_pb2.PlanRel(
            root=algebra_pb2.RelRoot(
                input=algebra_pb2.Rel(
                    read=algebra_pb2.ReadRel(
                        base_schema=type_pb2.NamedStruct(
                            names=['a', 'b'],
                            struct=type_pb2.Type.Struct(
                                types=[type_pb2.Type(i64=type_pb2.Type.I64()),
                                       type_pb2.Type(string=type_pb2.Type.String())])),
                        named_table=algebra_pb2.ReadRel.NamedTable(names=[table_name])
                    )),
                names=['a', 'b']))])
    return execute_plan(connection, plan)

def test_substrait_pyarrow(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (a integer, b varchar )')
    connection.execute('INSERT INTO integers VALUES (0, \'a\'),(1, \'b\')')
    arrow_table = connection.execute('FROM integers').arrow()

    connection.register("arrow_integers", arrow_table)
   
    arrow_result = execute_query(connection, "arrow_integers")

    assert connection.execute("FROM arrow_result").fetchall() == [(0, 'a'), (1, 'b')]
