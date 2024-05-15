import pytest
import duckdb

class TestSetOperation(object):
	def test_union(self, connection):
		connection.execute("""
			create table tbl1 as select * from (VALUES
				(1, 2, 3, 4),
				(2, 3, 4, 5),
				(3, 4, 5, 6)) as tbl(A, B, C, D)
		""")
		connection.execute("""
			create table tbl2 as select * from (VALUES
				(11, 12, 13, 14, 15),
				(12, 13, 14, 15, 16),
				(13, 14, 15, 16, 17)) as tbl (A, B, C, D, E)
		""")

		query = """
			select
				*
			from
				(
					select A, B, C, D, 0 as E from tbl1
				)
			union all (
				select * from tbl2
			) order by all
		"""
		expected = connection.execute(query).fetchall()
		json = connection.get_substrait_json(query).fetchall()[0][0]
		rel = connection.from_substrait_json(json)
		actual = rel.fetchall()
		assert expected == actual

	def test_except(self, connection):
		connection.execute("""
			create table tbl1 as select * from (VALUES
				(1, 2, 3, 4),
				(2, 3, 4, 5)
			) as tbl(A, B, C, D)
		""")
		connection.execute("""
			create table tbl2 as select * from (VALUES
				(2, 3, 4, 5),
				(3, 4, 5, 6)
			) as tbl(B, C, D, A)
		""")
		query = """
			select * from tbl1 EXCEPT (select * from tbl2);
		"""
		expected = connection.execute(query).fetchall()
		json = connection.get_substrait_json(query).fetchall()[0][0]
		rel = connection.from_substrait_json(json)
		actual = rel.fetchall()
		assert expected == actual

	def test_intersect(self, connection):
		connection.execute("""
			create table tbl1 as select * from (VALUES
				(1, 2, 3, 4),
				(2, 3, 4, 5)
			) as tbl(A, B, C, D)
		""")
		connection.execute("""
			create table tbl2 as select * from (VALUES
				(2, 3, 4, 5),
				(3, 4, 5, 6)
			) as tbl(B, C, D, A)
		""")
		query = """
			select * from tbl1 INTERSECT (select * from tbl2);
		"""
		expected = connection.execute(query).fetchall()
		json = connection.get_substrait_json(query).fetchall()[0][0]
		rel = connection.from_substrait_json(json)
		actual = rel.fetchall()
		assert expected == actual
