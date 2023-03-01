import duckdb
import pytest
from io import StringIO 

SubstraitCompiler = pytest.importorskip('ibis_substrait.compiler.core')
ibis = pytest.importorskip('ibis')
BaseBackend = pytest.importorskip('ibis.backends.base')
parse_type = pytest.importorskip('ibis.backends.duckdb.datatypes')
get_tpch_query = pytest.importorskip('ibis_tpch_util')

class IbisDuckDBTester:
	def __init__(self, tmp_path, initialization_queries):
		self.tmp_path = tmp_path
		self.con = duckdb.connect(tmp_path)

		# Initialize + populate the database
		for query in initialization_queries:
			self.con.sql(query)
		
		self.con.close()
		self.con = duckdb.connect(tmp_path, read_only=True)

		self.ibis_con = ibis.connect(f"duckdb://{tmp_path}", read_only=True)
	
	def test(self, expression_producer):
		expr = expression_producer(self.ibis_con)
		relation = self.generate_relation(expr.unbind())

		# Verify that the expressions produce the same result
		res = expr.to_pyarrow()
		duck_res = relation.arrow()
		#print(res)
		#print(duck_res)
		assert duck_res == res

class SQLIbisDuckDBTester(IbisDuckDBTester):
	def generate_relation(self, expr):
		# From the ibis expression - generate sql
		sql_string = StringIO()
		ibis.show_sql(expr, file=sql_string)

		# Then use the sql to generate a relation
		relation = self.con.sql(str(sql_string.getvalue()))
		return relation

	def __init__(self, tmp_path, initialization_queries):
		super(SQLIbisDuckDBTester, self).__init__(tmp_path, initialization_queries)

class SubstraitIbisDuckDBTester(IbisDuckDBTester):
	def generate_relation(self, expr):
		compiler = SubstraitCompiler.SubstraitCompiler()
		from google.protobuf import json_format

		proto = compiler.compile(expr)

		json_plan = json_format.MessageToJson(proto)
		return self.con.from_substrait_json(json_plan)

	def __init__(self, tmp_path, initialization_queries):
		super(SubstraitIbisDuckDBTester, self).__init__(tmp_path, initialization_queries)

def extract_year(ibis_db):
	tbl = ibis_db.table('tbl')
	return tbl[getattr(tbl.date, "year")().cast('int64')]

def extract_month(ibis_db):
	tbl = ibis_db.table('tbl')
	return tbl[getattr(tbl.date, "month")().cast('int64')]

class TestIbisRoundtrip(object):
	def test_extract(self, tmp_path):
		# Create a disk-backed duckdb database
		db_path = str(tmp_path / 'extract_db')

		tester = SubstraitIbisDuckDBTester(db_path, [
			"""
				create table tbl(date timestamp)
			""",
			"""
				insert into tbl values ('2021/09/21 12:02:21'::TIMESTAMP)
			"""
		])
		tester.test(extract_year)
		tester.test(extract_month)
