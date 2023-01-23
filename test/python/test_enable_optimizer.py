import pandas as pd
import duckdb

def test_optimizer_defaults_to_true(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5")
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", True)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 == proto_bytes2

def test_optimizer_with_abs(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2

def test_optimizer_with_like(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE varchars (v VARCHAR)')
    connection.execute('INSERT INTO varchars VALUES (\'ducky\'), (\'mcDuck\'), (\'Duckster\')')

    res1 = connection.get_substrait("SELECT * FROM varchars WHERE v LIKE '%y'", True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT * FROM varchars WHERE v LIKE '%y'", False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2