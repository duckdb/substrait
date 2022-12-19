import duckdb
import pytest

def test_substrait_json(require):
    connection = require('substrait')
    if connection is None:
        return

    connection.execute('CREATE TABLE integers (i integer)')
    json =  connection.get_substrait_json("select * from integers limit 5").fetchone()[0]
    expected_result = '{"relations":[{"root":{"input":{"fetch":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"count":"5"}},"names":["i"]}}]}'
    assert json == expected_result

    with pytest.raises(duckdb.CatalogException, match="Table with name p does not exist!"):
        connection.get_substrait_json("select * from p limit 5").fetchone()[0]

    # Test closed connection
    connection.close()
    with pytest.raises(duckdb.ConnectionException, match="Connection has already been closed"):
        connection.get_substrait_json("select * from integers limit 5")


def test_stack_not_deep_enough(require):
    con = require('substrait')
    for i in range(0,1000):
        with pytest.raises(duckdb.CatalogException, match="Table with name p does not exist!"):
            con.get_substrait_json("select * from p limit 5")