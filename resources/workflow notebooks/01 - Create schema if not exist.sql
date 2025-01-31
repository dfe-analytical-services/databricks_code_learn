-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create schema if not exists
-- MAGIC
-- MAGIC This notebook takes two parameters which are passed down from the job parameters set in the workflow:
-- MAGIC
-- MAGIC - `catalog`
-- MAGIC - `owner_group`
-- MAGIC
-- MAGIC Within the `catalog` it will create a schema if it doesn't already exist titled `shared`. 
-- MAGIC
-- MAGIC It will then set the schema ownership to the group specified in the `owner_group` parameter. This is a necessary step if you want other users of the catalog to be able to interact with the tables / views / data held in the `catalog`. 
-- MAGIC
-- MAGIC This is because the default behaviour of Databricks is to set the schema ownership to the account of the person that created it. However for this purpose (and many others) we need other users of the catalog to be able to interact with it, so we will set the owner to the group that oversees the management of the catalog.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tell Databricks which catalog to use
-- MAGIC
-- MAGIC The chunk below specifies which catalog the code should be ran in.
-- MAGIC
-- MAGIC Note that here we're using the syntax `${parameter_name}`. Technically this syntax for parameters is deprecated on Databricks Run time (DBR) 15+ and you will likely receive a warning. The correct syntax for DBR 15+ should be `IDENTIFIER(:parameter_name)` however for the `USE CATALOG` and `ALTER TABLE table OWNER TO` commands the new syntax doesn't work as intended. Thankfully the deprecated syntax does still work on DBR 15+ so that is why we are using it here.

-- COMMAND ----------

USE CATALOG ${catalog};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create schema if not exists
-- MAGIC
-- MAGIC Here we create a schema called `shared` if it doesn't already exist. `IF NOT EXISTS` is used here because if the schema does already exist we don't want to recreate it and lose all the data that is currently in there. We also don't want an error telling us the schema already exists as that would end the workflow.
-- MAGIC
-- MAGIC > If you wanted to change this so that it always recreated and overwrote the schema you could instead use the syntax `CREATE OR REPLACE SCHEMA schema_name;`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS shared;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Changing schema ownership
-- MAGIC
-- MAGIC This last step is to ensure that other users of the catalog can interact with the schema. By default the schema's owner is the person that creates it but for team working we need everyone with access to the catalog to be able to access the contents of this schema, so we're going to set the ownership to the Read / Write / Create group that manages the database. This group was setup by the ADA team to grant your team access to the data catalog and so anyone with access to the catalog will be in that group and will have the ability to interact and modify the schema as required.

-- COMMAND ----------

ALTER SCHEMA shared OWNER TO ${owner_group};
