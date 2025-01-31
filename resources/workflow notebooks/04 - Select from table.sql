-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #Select from table
-- MAGIC
-- MAGIC This notebook uses only one parameter which is passed down from the job parameters set in the workflow:
-- MAGIC
-- MAGIC - `catalog`
-- MAGIC
-- MAGIC Within the `catalog` it will use the schema created in the previous task titled `shared`. 
-- MAGIC
-- MAGIC It will then select all data in the `shared_table_runs` table created in the previous notebook task.
-- MAGIC
-- MAGIC ## Use Catalog
-- MAGIC
-- MAGIC Here, once again we need to tell Databricks where in the unity catalog we want to perform the actions in this notebook and again we will specify the catalog and schema.

-- COMMAND ----------

USE CATALOG ${catalog};
USE shared;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Select all
-- MAGIC
-- MAGIC The chunk below just selects all the rows from the table created earlier. If you are the first person in your team to run this workflow, you will likely only see one row which will relate to yourself.
-- MAGIC
-- MAGIC If others in your team have run the workflow before you, you should be able to see their names and the time(s) which they have run the workflow previously.

-- COMMAND ----------

SELECT * FROM shared_table_runs ORDER BY time_of_run;
