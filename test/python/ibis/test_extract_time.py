import duckdb
import pytest

from ibis_duckdb_tester import CombinedIbisDuckDBTester, SubstraitIbisDuckDBTester
ibis = pytest.importorskip('ibis')

def extract_component(ibis_db, named_component):
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.d.time(), named_component)().cast('int64')]
	return expr

class TestIbisExtractTime(object):
	def test_extract_combined(self, tmp_path, require):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = CombinedIbisDuckDBTester(db_path, [
			"""
				create table tbl(d timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		], require)

		# See issue: https://github.com/ibis-project/ibis/issues/5649
		# timestamp.time() can't translate to SQL
		time_components = [
			#"hour",
			#"millisecond",
			#"minute",
			#"second"
		]
		for component in time_components:
			tester.test(extract_component, component)

	def test_extract_substrait(self, tmp_path, require):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = SubstraitIbisDuckDBTester.create(db_path, [
			"""
				create table tbl(d timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		], require)

		# None of these are mapped yet as of ibis_substrait 2.22.0
		time_components = [
			#"hour",
			#"millisecond",
			#"minute",
			#"second"
		]
		for component in time_components:
			tester.test(extract_component, component)
