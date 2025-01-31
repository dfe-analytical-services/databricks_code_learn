-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Create table if not exists
-- MAGIC
-- MAGIC This notebook uses only one parameter which is passed down from the job parameters set in the workflow:
-- MAGIC
-- MAGIC - `catalog`
-- MAGIC
-- MAGIC Within the `catalog` it will use the schema created in the previous task titled `shared`. 
-- MAGIC
-- MAGIC It will then create a table titled `shared_table_runs` in the schema if it doesn't already exist.
-- MAGIC
-- MAGIC ## Use Catalog
-- MAGIC
-- MAGIC Here, once again we need to tell Databricks where in the unity catalog we want to perform the actions in this notebook.
-- MAGIC
-- MAGIC This time we want to set the schema to work in as well though. To do this we can use the syntax `USE schema_name;` after setting the catalog.

-- COMMAND ----------

USE CATALOG ${catalog};
USE shared;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create table if not exists
-- MAGIC
-- MAGIC Like in the previous notebook we don't want to destroy and recreate the table if it already exists, as we'd lose all of the data in it. Similarly we don't want an error if it already exists as it would break the workflow, so we use the `IF NOT EXISTS` statement again.
-- MAGIC
-- MAGIC > As with the schema if you wanted to recreate the table everytime this script was run you could instead use the syntax `CREATE OR REPLACE TABLE table_name`.
-- MAGIC
-- MAGIC It's often a good idea to set an `IDENTITY` column which will give each row a unique number to identify it as, which is done here on the `id` column.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS shared_table_runs (
  id bigint NOT NULL GENERATED ALWAYS AS IDENTITY,
  name string,
  time_of_run timestamp
)
