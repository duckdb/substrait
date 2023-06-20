from unicodedata import name
import duckdb
import pytest

from ibis_duckdb_tester import CombinedIbisDuckDBTester
ibis = pytest.importorskip('ibis')

def extract_component(ibis_db, named_component):
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.date, named_component)().cast('int64')]
	return expr


class TestIbisExtractTimestamp(object):
	@pytest.mark.parametrize('component', [
		"day",
		"day_of_year",
		"epoch_seconds",
		"month",
		"quarter",
		"week_of_year",
		"year",
		"day_of_week", # not sure if this one should be in this list at all
		"hour",
		"millisecond",
		"minute",
		"second"
	])
	def test_extract(self, tmp_path, require, component):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		# NOTE: support for 'extract' seems to be removed for timestamp
		if component not in [
			#"day",
			#"month",
			#"year"
		]:
			pytest.skip(f'{component} not tested (yet)')
		tester = CombinedIbisDuckDBTester(db_path, [
			"""
				create table tbl(date timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		], require)

		tester.test(extract_component, component)
