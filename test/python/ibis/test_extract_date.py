import duckdb
import pytest

from ibis_duckdb_tester import CombinedIbisDuckDBTester, SubstraitIbisDuckDBTester
ibis = pytest.importorskip('ibis')

def extract_component(ibis_db, named_component):
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.d, named_component)().cast('int64')]
	return expr

class TestIbisExtractTime(object):
	def test_extract_combined(self, tmp_path, require):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = CombinedIbisDuckDBTester(db_path, [
			"""
				create table tbl(d date)
			""",
			"""
				insert into tbl values ('2021/09/21'::DATE)
			"""
		], require)

		date_components = [
			"day",
			#"day_of_year",
			#"epoch_seconds",
			"month",
			#"quarter",
			#"week_of_year",
			"year"
		]
		for component in date_components:
			tester.test(extract_component, component)
