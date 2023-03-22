from unicodedata import name
import duckdb
import pytest

from ibis_duckdb_tester import CombinedIbisDuckDBTester
ibis = pytest.importorskip('ibis')

def extract_component(ibis_db, named_component):
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.date, named_component)().cast('int64')]
	return expr

# NOTE: Ibis does not implement 'timestamp_tz' (yet?)
class TestIbisExtractTimestampTZ(object):
	def test_extract(self, tmp_path, require):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = CombinedIbisDuckDBTester(db_path, [
			"""
				create table tbl(date timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		], require)

		timestamp_tz_components = [
			#"day",
			#"day_of_year",
			#"epoch_seconds",
			#"month",
			#"quarter",
			#"week_of_year",
			#"year",
			#"day_of_week", # not sure if this one should be in this list at all
			#"hour",
			#"millisecond",
			#"minute",
			#"second"
		]
		for component in timestamp_tz_components:
			tester.test(extract_component, component)
