import duckdb
import pytest

def test_substrait_json(require):
    connection = require('substrait')
    if connection is None:
        return

    connection.execute('CREATE TABLE integers (i integer)')
    json =  connection.get_substrait_json("select * from integers limit 5").fetchone()[0]
    expected_result = '{"relations":[{"root":{"input":{"fetch":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"count":"5"}},"names":["i"]}}],"version":{"minorNumber":48,"producer":"DuckDB"}}'
    assert json == expected_result

