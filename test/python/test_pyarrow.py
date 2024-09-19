from pathlib import Path

import duckdb
import pyarrow as pa
from substrait.gen.proto import algebra_pb2, plan_pb2, type_pb2


def create_connection() -> duckdb.DuckDBPyConnection:
    """Create a connection to the backend."""
    connection = duckdb.connect(config={'max_memory': '100GB',
                                        "allow_unsigned_extensions": "true",
                                        'temp_directory': str(Path('.').resolve())})
    connection.install_extension('substrait')
    connection.load_extension('substrait')

    return connection


def execute_plan(connection: duckdb.DuckDBPyConnection, plan: plan_pb2.Plan) -> pa.lib.Table:
    """Execute the given Substrait plan against DuckDB."""
    plan_data = plan.SerializeToString()

    try:
        query_result = connection.from_substrait(proto=plan_data)
    except Exception as err:
        raise ValueError(f'DuckDB Execution Error: {err}') from err
    return query_result.arrow()


def register_table(
        connection: duckdb.DuckDBPyConnection,
        table_name: str,
        location: Path,
        use_duckdb_python_api: bool = True) -> None:
    """Register the given table with the backend."""
    if use_duckdb_python_api:
        table_data = connection.read_parquet(location)
        connection.register(table_name, table_data)
    else:
        files_sql = f"CREATE OR REPLACE TABLE {table_name} AS FROM read_parquet(['{location}'])"
        connection.execute(files_sql)


def register_table_with_arrow_data(
        connection: duckdb.DuckDBPyConnection,
        table_name: str,
        data: bytes) -> None:
    """Register the given arrow data as a table with the backend."""
    r = pa.ipc.open_stream(data).read_all()
    connection.register(table_name, r)


def describe_table(connection, table_name: str):
    s = connection.execute(f"SELECT * FROM {name}")
    t = connection.table(name)
    v = connection.view(name)
    print(f's = %s' % s.fetch_arrow_table())
    print(f't = %s' % t)
    print(f'v = %s' % v)

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
                        named_table=algebra_pb2.ReadRel.NamedTable(names=[name])
                    )),
                names=['a', 'b']))])
    print('About to execute Substrait')
    x = execute_plan(connection, plan)
    print(f'x = %s' % x)


def serialize_table(table: pa.Table) -> bytes:
    """Serialize a PyArrow table to bytes."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


if __name__ == '__main__':
    connection = create_connection()
    name = 'my_table'

    use_parquet = False
    if use_parquet:
        register_table(connection, name,
                       '/Users/davids/projects/voltrondata-spark-substrait-gateway/third_party/tpch/parquet/customer/part-0.parquet')
    else:
        table = pa.table({'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']})
        serialized_data = serialize_table(table)
        register_table_with_arrow_data(connection, name, serialized_data)

    describe_table(connection, name)

    connection.close()