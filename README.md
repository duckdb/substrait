# Substrait - DuckDB
Substrait - DuckDB is an extension that provides substrait support to [DuckDB](https://www.duckdb.org).
The main goal of this extension is to support a both production and consumption of substrait query plans in DuckDB.



To build, type 
```
make
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb 
```

Then, load the Substrait - DuckDB extension like so:
```SQL
LOAD 'build/release/substrait.duckdb_extension';
```

## Support
This extension is mainly supported in 3 different APIs. 1) The SQL API, 2) The Python API, 3) The R API.
Here we depict how to consume and produce substrait query plans in each API.

### SQL
In the SQL API, users can generate substrait plans (into a blob or a JSON) and consume substrait plans.

1) Blob Generation
     
     To generate a substrait blob the ```get_substrait(SQL)``` function must be called with a valid SQL select query.
     ```sql
     CREATE TABLE crossfit (exercise text,dificulty_level int);
     INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);
     
     CALL get_substrait('select count(exercise) as exercise from crossfit where dificulty_level <=5')
     ----
     \x12\x09\x1A\x07\x10\x01\x1A\x03lte\x12\x11\x1A\x0F\x10\x02\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x03\x1A\x03and\x12\x10\x1A\x0E\x10\x04\x1A\x0Acount_star\x1A\xCB\x01\x12\xC8\x01\x0A\xBB\x01:\xB8\x01\x12\xAB\x01"\xA8\x01\x12\x97\x01\x0A\x94\x01\x12.\x0A\x08exercise\x0A\x0Fdificulty_level\x12\x11\x0A\x07\xB2\x01\x04\x08\x0D\x18\x01\x0A\x04*\x02\x10\x01\x18\x02\x1AJ\x1AH\x08\x03\x1A\x04\x0A\x02\x10\x01""\x1A \x1A\x1E\x08\x01\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x06\x1A\x04\x0A\x02(\x05"\x1A\x1A\x18\x1A\x16\x08\x02\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x0A\x0A\x06\x0A\x02\x08\x01\x0A\x00\x10\x01:\x0A\x0A\x08crossfit\x1A\x00"\x0A\x0A\x08\x08\x04*\x04:\x02\x10\x01\x1A\x08\x12\x06\x0A\x02\x12\x00"\x00\x12\x08exercise
     ```
2) Json Generation
     
     To generate a substrait blob the ```get_substrait_json(SQL)``` function must be called with a valid SQL select query.
     ```sql
     CALL get_substrait_json('select count(exercise) as exercise from crossfit where dificulty_level <=5')
     ----
     {"extensions":[{"extensionFunction":{"functionAnchor":1,"name":"lte"}},{"extensionFunction":{"functionAnchor":2,"name":"is_not_null"}},{"extensionFunction":{"functionAnchor":3,"name":"and"}},{"extensionFunction":{"functionAnchor":4,"name":"count_star"}}],"relations":[{"root":{"input":{"project":{"input":{"aggregate":{"input":{"read":{"baseSchema":{"names":["exercise","dificulty_level"],"struct":{"types":[{"varchar":{"length":13,"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"filter":{"scalarFunction":{"functionReference":3,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"scalarFunction":{"functionReference":1,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}},{"value":{"literal":{"i32":5}}}]}}},{"value":{"scalarFunction":{"functionReference":2,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}}]}},"projection":{"select":{"structItems":[{"field":1},{}]},"maintainSingularStruct":true},"namedTable":{"names":["crossfit"]}}},"groupings":[{}],"measures":[{"measure":{"functionReference":4,"outputType":{"i64":{"nullability":"NULLABILITY_NULLABLE"}}}}]}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["exercise"]}}]}
     ```
3) Blob Consumption
     
     To consume a substrait blob the ```from_substrait(blob)``` function must be called with a valid substrait BLOB plan.
     ```sql
     CALL get_substrait('select count(exercise) as exercise from crossfit where dificulty_level <=5')
     ----
     \x12\x09\x1A\x07\x10\x01\x1A\x03lte\x12\x11\x1A\x0F\x10\x02\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x03\x1A\x03and\x12\x10\x1A\x0E\x10\x04\x1A\x0Acount_star\x1A\xCB\x01\x12\xC8\x01\x0A\xBB\x01:\xB8\x01\x12\xAB\x01"\xA8\x01\x12\x97\x01\x0A\x94\x01\x12.\x0A\x08exercise\x0A\x0Fdificulty_level\x12\x11\x0A\x07\xB2\x01\x04\x08\x0D\x18\x01\x0A\x04*\x02\x10\x01\x18\x02\x1AJ\x1AH\x08\x03\x1A\x04\x0A\x02\x10\x01""\x1A \x1A\x1E\x08\x01\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x06\x1A\x04\x0A\x02(\x05"\x1A\x1A\x18\x1A\x16\x08\x02\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x0A\x0A\x06\x0A\x02\x08\x01\x0A\x00\x10\x01:\x0A\x0A\x08crossfit\x1A\x00"\x0A\x0A\x08\x08\x04*\x04:\x02\x10\x01\x1A\x08\x12\x06\x0A\x02\x12\x00"\x00\x12\x08exercise
     ```


### Python

### R

