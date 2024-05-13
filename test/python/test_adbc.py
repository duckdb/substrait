import duckdb
import pytest
import sys
import datetime
import os
from os.path import abspath, join, dirname, normpath
import glob


adbc_driver_manager = pytest.importorskip("adbc_driver_manager.dbapi")
adbc_driver_manager_lib = pytest.importorskip("adbc_driver_manager._lib")
json_format = pytest.importorskip("google.protobuf.json_format")
plan_pb2 = pytest.importorskip("substrait.gen.proto.plan_pb2")
pyarrow = pytest.importorskip("pyarrow")

# When testing local, if you build via BUILD_PYTHON=1 make, you need to manually set up the
# dylib duckdb path.
driver_path = duckdb.duckdb.__file__

def find_substrait():
    # Paths to search for extensions
    build = normpath(join(dirname(__file__), "../../duckdb/build/"))
    extension = "extension/*/*.duckdb_extension"

    extension_search_patterns = [
        join(build, "release", extension),
        join(build, "debug", extension),
    ]

    # DUCKDB_PYTHON_TEST_EXTENSION_PATH can be used to add a path for the extension test to search for extensions
    if 'DUCKDB_PYTHON_TEST_EXTENSION_PATH' in os.environ:
        env_extension_path = os.getenv('DUCKDB_PYTHON_TEST_EXTENSION_PATH')
        env_extension_path = env_extension_path.rstrip('/')
        extension_search_patterns.append(env_extension_path + '/*/*.duckdb_extension')
        extension_search_patterns.append(env_extension_path + '/*.duckdb_extension')

    extension_paths_found = []
    for pattern in extension_search_patterns:
        extension_pattern_abs = abspath(pattern)
        for path in glob.glob(extension_pattern_abs):
            extension_paths_found.append(path)

    for path in extension_paths_found:
        if path.endswith("substrait.duckdb_extension"):
            return path
    pytest.skip(f'could not load substrait')

    return "Fail"


@pytest.fixture
def duck_conn():
    with adbc_driver_manager.connect(driver=driver_path, entrypoint="duckdb_adbc_init", db_kwargs={"allow_unsigned_extensions": "true"}) as conn:
        yield conn

file_path = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(file_path,'data','somefile.parquet')

PLAN_PROTOTEXT = '''{
  "relations":[
    {
      "root":{
        "input":{
          "project":{
            "input":{
              "read":{
                "baseSchema":{
                  "names":[
                    "mbid",
                    "artist_mb"
                  ],
                  "struct":{
                    "types":[
                      {
                        "string":{
                          "nullability":"NULLABILITY_NULLABLE"
                        }
                      },
                      {
                        "string":{
                          "nullability":"NULLABILITY_NULLABLE"
                        }
                      }
                    ],
                    "nullability":"NULLABILITY_REQUIRED"
                  }
                },
                "projection":{
                  "select":{
                    "structItems":[
                      {
                        
                      },
                      {
                        "field":1
                      }
                    ]
                  },
                  "maintainSingularStruct":true
                },
                "localFiles":{
                  "items":[
                    {
                      "uriFile":"''' + file_path + '''",
                      "parquet":{
                        
                      }
                    }
                  ]
                }
              }
            },
            "expressions":[
              {
                "selection":{
                  "directReference":{
                    "structField":{
                      
                    }
                  },
                  "rootReference":{
                    
                  }
                }
              },
              {
                "selection":{
                  "directReference":{
                    "structField":{
                      "field":1
                    }
                  },
                  "rootReference":{
                    
                  }
                }
              }
            ]
          }
        },
        "names":[
          "mbid",
          "artist_mb"
        ]
      }
    }
  ],
  "version":{
    "minorNumber":48,
    "producer":"DuckDB"
  }
}'''

def test_substrait_over_adbc(duck_conn):
    plan = json_format.Parse(PLAN_PROTOTEXT, plan_pb2.Plan())
    cur = duck_conn.cursor()
    substrait_path = find_substrait()
    cur.execute("LOAD '"+ substrait_path + "'")
    
    plan_data = plan.SerializeToString()
    cur.execute(plan_data)
    result_table = cur.fetch_arrow_table()
    correct_table = pyarrow.Table.from_pydict({
        'mbid': pyarrow.array(["1"], type=pyarrow.string()),
        'artist_mb': pyarrow.array(["Tenacious D"], type=pyarrow.string())
    })
    assert result_table.equals(correct_table)
