import duckdb
import pytest
from io import StringIO 

SubstraitCompiler = pytest.importorskip('ibis_substrait.compiler.core')
ibis = pytest.importorskip('ibis')

def initialize_db(path, queries):
	con = duckdb.connect(path)
	# Initialize + populate the database
	for query in queries:
		con.sql(query)
	con.close()

class IbisDuckDBTester:
	def __init__(self, path):
		self.path = path

	def initialize(self, queries):
		initialize_db(self.path, queries)
		
	def open(self):
		# Create connections to the db
		self.con = duckdb.connect(self.path, read_only=True)
		self.ibis_con = ibis.connect(f"duckdb://{self.path}", read_only=True)

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
	def create(path, queries):
		self = SQLIbisDuckDBTester(path)
		self.initialize(queries)
		self.open()
		return self

	def generate_relation(self, expr):
		# From the ibis expression - generate sql
		sql_string = StringIO()
		ibis.show_sql(expr, file=sql_string)

		# Then use the sql to generate a relation
		relation = self.con.sql(str(sql_string.getvalue()))
		return relation

	def __init__(self, path):
		super(SQLIbisDuckDBTester, self).__init__(path)

class SubstraitIbisDuckDBTester(IbisDuckDBTester):
	def create(path, queries):
		self = SubstraitIbisDuckDBTester(path)
		self.initialize(queries)
		self.open()
		return self

	def generate_relation(self, expr):
		compiler = SubstraitCompiler.SubstraitCompiler()
		from google.protobuf import json_format

		proto = compiler.compile(expr)

		json_plan = json_format.MessageToJson(proto)
		return self.con.from_substrait_json(json_plan)

	def __init__(self, path):
		super(SubstraitIbisDuckDBTester, self).__init__(path)

class CombinedIbisDuckDBTester():
	def __init__(self, path, queries):
		self.path = path

		self.testers = []
		self.testers += [SQLIbisDuckDBTester(self.path)]
		self.testers += [SubstraitIbisDuckDBTester(self.path)]

		initialize_db(self.path, queries)

		for tester in self.testers:
			tester.open()
	
	def test(self, expression_producer):
		for tester in self.testers:
			tester.test(expression_producer)

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
