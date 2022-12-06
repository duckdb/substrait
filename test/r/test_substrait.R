library("DBI")
library("testthat")

load_extension <- function() {
  con <- dbConnect(duckdb::duckdb(config=list("allow_unsigned_extensions"="true")))
  dbExecute(con, "LOAD '../../build/release/extension/substrait/substrait.duckdb_extension';")
  return (con)
}

test_that("substrait extension test", {
  con <- load_extension()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbExecute(con, "CREATE TABLE integers (i INTEGER)")
  dbExecute(con, "INSERT INTO integers VALUES (42)")
  plan <- duckdb::duckdb_get_substrait(con, "select * from integers limit 5")
  result <- duckdb::duckdb_prepare_substrait(con, plan)
  df <- dbFetch(result)
  expect_equal(df$i, 42L)

  result_arrow <- duckdb::duckdb_prepare_substrait(con, plan, TRUE)
  df2 <- as.data.frame(duckdb::duckdb_fetch_arrow(result_arrow))
  expect_equal(df2$i, 42L)
})

test_that("substrait extension json test", {
  con <- load_extension()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbExecute(con, "CREATE TABLE integers (i INTEGER)")
  expected_json <- "{\"relations\":[{\"root\":{\"input\":{\"fetch\":{\"input\":{\"project\":{\"input\":{\"read\":{\"baseSchema\":{\"names\":[\"i\"],\"struct\":{\"types\":[{\"i32\":{\"nullability\":\"NULLABILITY_NULLABLE\"}}],\"nullability\":\"NULLABILITY_REQUIRED\"}},\"projection\":{\"select\":{\"structItems\":[{}]},\"maintainSingularStruct\":true},\"namedTable\":{\"names\":[\"integers\"]}}},\"expressions\":[{\"selection\":{\"directReference\":{\"structField\":{}},\"rootReference\":{}}}]}},\"count\":\"5\"}},\"names\":[\"i\"]}}]}"
  json <- duckdb::duckdb_get_substrait_json(con, "select * from integers limit 5")
  expect_equal(json, expected_json)
})

test_that("substrait extension from json test", {
  con <- load_extension()
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbExecute(con, "CREATE TABLE integers (i INTEGER)")
  dbExecute(con, "INSERT INTO integers VALUES (42)")
  json <- "{\"relations\":[{\"root\":{\"input\":{\"fetch\":{\"input\":{\"project\":{\"input\":{\"read\":{\"baseSchema\":{\"names\":[\"i\"],\"struct\":{\"types\":[{\"i32\":{\"nullability\":\"NULLABILITY_NULLABLE\"}}],\"nullability\":\"NULLABILITY_REQUIRED\"}},\"projection\":{\"select\":{\"structItems\":[{}]},\"maintainSingularStruct\":true},\"namedTable\":{\"names\":[\"integers\"]}}},\"expressions\":[{\"selection\":{\"directReference\":{\"structField\":{}},\"rootReference\":{}}}]}},\"count\":\"5\"}},\"names\":[\"i\"]}}]}"
  
  result <- duckdb::duckdb_prepare_substrait_json(con, json)
  df <- dbFetch(result)
  expect_equal(df$i, 42L)
})