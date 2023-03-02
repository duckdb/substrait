import duckdb
import pytest

from ibis_duckdb_tester import CombinedIbisDuckDBTester
ibis = pytest.importorskip('ibis')

def extract_year(ibis_db):
	"""
		SELECT
		CAST(CAST(EXTRACT(year FROM t0.date) AS SMALLINT) AS BIGINT) AS "Cast(ExtractYear(date), int64)"
		FROM tbl AS t0
	"""
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.date, "year")().cast('int64')]
	return expr

def extract_month(ibis_db):
	"""
		SELECT
		CAST(CAST(EXTRACT(month FROM t0.date) AS SMALLINT) AS BIGINT) AS "Cast(ExtractMonth(date), int64)"
		FROM tbl AS t0
	"""
	tbl = ibis_db.table('tbl')
	expr = tbl[getattr(tbl.date, "month")().cast('int64')]
	return expr

class TestIbisRoundtrip(object):
	def test_extract(self, tmp_path):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = CombinedIbisDuckDBTester(db_path, [
			"""
				create table tbl(date timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		])
		tester.test(extract_year)
		tester.test(extract_month)
