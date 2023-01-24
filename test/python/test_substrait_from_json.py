import duckdb
import pytest

def test_substrait_from_json(require):
    connection = require('substrait')
    if connection is None:
        return

    connection.execute('CREATE TABLE integers (i integer)')
    connection.execute('INSERT INTO integers values (0)')
    
    query_json = '{"relations":[{"root":{"input":{"fetch":{"input":{"project":{"input":{"read":{"baseSchema":{"names":["i"],"struct":{"types":[{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"projection":{"select":{"structItems":[{}]},"maintainSingularStruct":true},"namedTable":{"names":["integers"]}}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"count":"5"}},"names":["i"]}}]}'
    
    assert connection.from_substrait_json(query_json).fetchone()[0] == 0

    # Test malformed json
    with pytest.raises(Exception, match="Was not possible to convert"):
        query_json = '{"relations":[{"ro}'
        connection.from_substrait_json(query_json).fetchone()[0]
        
    # Test closed connection
    connection.close()
    with pytest.raises(duckdb.ConnectionException, match="Connection has already been closed"):
        connection.from_substrait_json(query_json).fetchone()[0]

