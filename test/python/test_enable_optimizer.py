import pandas as pd
import duckdb
import pytest

def test_optimizer_abs_broken_result(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    # Insert only positive values
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    # Generate query plan with optimizer enabled
    res1 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5")
    proto_bytes1 = res1.fetchone()[0]

    # Now insert a negative value
    connection.execute('INSERT INTO integers VALUES (-1)')

    # Use the previously generated query plan
    incorrect_result = connection.from_substrait(proto_bytes1).fetchall()
    print(incorrect_result)
    assert(incorrect_result == [(0,), (1,), (2,), (3,), (4,)])

def test_optimizer_defaults_to_true(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5")
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=True)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 == proto_bytes2

def test_optimizer_with_abs(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2

def test_optimizer_with_like(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE varchars (v VARCHAR)')
    connection.execute('INSERT INTO varchars VALUES (\'ducky\'), (\'mcDuck\'), (\'Duckster\')')

    res1 = connection.get_substrait("SELECT * FROM varchars WHERE v LIKE '%y'", enable_optimizer=True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait("SELECT * FROM varchars WHERE v LIKE '%y'", enable_optimizer=False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2

# Now test get_substrait_json
def test_optimizer_json_defaults_to_true(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait_json("SELECT abs(i) FROM integers LIMIT 5")
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait_json("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=True)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 == proto_bytes2

def test_optimizer_json_with_abs(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE integers (i INTEGER)')
    connection.execute('INSERT INTO integers VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),(NULL)')

    res1 = connection.get_substrait_json("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait_json("SELECT abs(i) FROM integers LIMIT 5", enable_optimizer=False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2

def test_optimizer_json_with_like(require):
    connection = require('substrait')

    connection.execute('CREATE TABLE varchars (v VARCHAR)')
    connection.execute('INSERT INTO varchars VALUES (\'ducky\'), (\'mcDuck\'), (\'Duckster\')')

    res1 = connection.get_substrait_json("SELECT * FROM varchars WHERE v LIKE '%y'", enable_optimizer=True)
    proto_bytes1 = res1.fetchone()[0]

    res2 = connection.get_substrait_json("SELECT * FROM varchars WHERE v LIKE '%y'", enable_optimizer=False)
    proto_bytes2 = res2.fetchone()[0]

    assert proto_bytes1 != proto_bytes2