import duckdb
import pytest

substrait_validator = pytest.importorskip('substrait_validator')

def run_substrait_validator(con, query):
    try:
        con.table('lineitem')
    except:
        con.execute(f"CALL dbgen(sf=0.01)")
    c = substrait_validator.Config()
    #  not yet implemented: typecast validation rules are not yet implemented
    c.override_diagnostic_level(1, "warning", "info")
    #  validator limitation: did not attempt to resolve YAML
    c.override_diagnostic_level(2001, "warning", "info")
    # too few field names
    c.override_diagnostic_level(4003, "error", "info")
    # Validator being of a different version than substrait
    c.override_diagnostic_level(7, "warning", "info")
    try:
        proto = con.get_substrait(query).fetchone()[0]
    except Exception as err:
        raise ValueError("DuckDB Compilation: " + str(err))
    substrait_validator.check_plan_valid(proto, config=c)
    
def run_tpch_validator(require, query_number):
    con = require('substrait', 'test.db')
    if not con:
        return
    query = con.execute("select query from tpch_queries() where query_nr="+str(query_number)).fetchone()[0]

    run_substrait_validator(con,query)

@pytest.mark.parametrize('query_number', [1,3,5,6,7,8,9,10,11,12,13,14,15,18,19])
def test_substrait_tpch_validator(require,query_number):
    run_tpch_validator(require,query_number)

@pytest.mark.skip(reason="DuckDB Compilation: INTERNAL Error: Unsupported join type MARK")
def test_substrait_tpch_validator_16(require):
    run_tpch_validator(require,16)

@pytest.mark.skip(reason="Skipping this test for now because it is part of the big posref refactoring")
def test_substrait_tpch_validator_18(require):
    run_tpch_validator(require,18)

@pytest.mark.skip(reason="Skipping this test for now because it is part of the big posref refactoring")
def test_substrait_tpch_validator_2(require):
    run_tpch_validator(require,2)

@pytest.mark.skip(reason="Skipping this test for now because it is part of the big posref refactoring")
def test_substrait_tpch_validator_17(require):
    run_tpch_validator(require,17)

@pytest.mark.skip(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
def test_substrait_tpch_validator_04(require):
    run_tpch_validator(require,4)

@pytest.mark.skip(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
def test_substrait_tpch_validator_20(require):
    run_tpch_validator(require,20)

@pytest.mark.skip(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
def test_substrait_tpch_validator_21(require):
    run_tpch_validator(require,21)

@pytest.mark.skip(reason="DuckDB Compilation: INTERNAL Error: INTERNAL Error: DELIM_JOIN")
def test_substrait_tpch_validator_22(require):
    run_tpch_validator(require,22)