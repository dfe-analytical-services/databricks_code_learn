-- Databricks notebook source
-- MAGIC %md
-- MAGIC %md
-- MAGIC #Insert data
-- MAGIC
-- MAGIC This notebook uses only one parameter which is passed down from the job parameters set in the workflow:
-- MAGIC
-- MAGIC - `catalog`
-- MAGIC
-- MAGIC Within the `catalog` it will use the schema created in the previous task titled `shared`. 
-- MAGIC
-- MAGIC It will then insert data into the `shared_table_runs` created in the previous notebook task. 
-- MAGIC
-- MAGIC ## Use Catalog
-- MAGIC
-- MAGIC Here, once again we need to tell Databricks where in the unity catalog we want to perform the actions in this notebook and again we will specify the catalog and schema.

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE shared;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Insert Into
-- MAGIC
-- MAGIC Now we can insert the `CURRENT_USER()` and `CURRENT_TIMESTAMP()` into the table. These are functions of SQL that are relatively self-explanatory.
-- MAGIC
-- MAGIC We only insert to the `name` and `time_of_run` columns here as the `id` column is automatically populated with a unique number by the catalog to ensure each row has a unique identifier.

-- COMMAND ----------

INSERT INTO shared_table_runs (name, time_of_run)
VALUES(CURRENT_USER(), CURRENT_TIMESTAMP());
