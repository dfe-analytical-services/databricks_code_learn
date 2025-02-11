-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Importing data
-- MAGIC
-- MAGIC In the previous notebook we transferred data from one data catalog to another, and generally this should be our main way of moving data long-term as the Department's data will eventually all be held in the Databricks 'Unity catalog'.
-- MAGIC
-- MAGIC However, there may be times when you need to import some data, either from a source you've found online or to migrate existing data from your current modelling area.
-- MAGIC
-- MAGIC For this we will need to import data into Databricks so we can use it.
-- MAGIC
-- MAGIC To practice with there is an [AirBnB opendata](https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata/data) set sourced from the [Kaggle website](https://www.kaggle.com/).
-- MAGIC
-- MAGIC For this task we're going to use the SQL language, the following notebook will cover how to do import data using `R` / `sparklyr`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Set your catalog and schema
-- MAGIC
-- MAGIC We're going to want to make sure that everything is written to the schema created in the last Notebook, so we'll want a widget again called schema. To make things easier in this workbook we'll also create a widget for the catalog.
-- MAGIC
-- MAGIC
-- MAGIC As this Notebook is in SQL we'll use the SQL syntax for creating and referring to widgets.

-- COMMAND ----------

--Creates a text box widget called schema, with a default value of nothing
CREATE WIDGET TEXT schema DEFAULT '';
--Creates a text box widget called catalog, with a default value of nothing
CREATE WIDGET TEXT catalog DEFAULT '';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Fill in the `schema` widget with the name you used in the last notebook and the `catalog` widget with the catalog for your team. 
-- MAGIC
-- MAGIC Then run the next chunk to tell the notebook which catalog and schema to run all of the commands below it in.

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);
USE IDENTIFIER(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Read in the CSV
-- MAGIC Now we'll read in the CSV data to SQL using the `read_files()` function. 
-- MAGIC
-- MAGIC The `read_files()` function will only accept an **absolute** path rather than a relative path. 
-- MAGIC
-- MAGIC Assuming that you have cloned the `databricks_code_learn` repository into your home directory on DataBricks the path will be:
-- MAGIC
-- MAGIC file:///Workspace/Users/`[your-email-address]`/databricks_code_learn/resources/Airbnb_Open_Data.csv
-- MAGIC
-- MAGIC If this is not where this repository is stored you will need to adjust the path in the code below accordingly.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Absolute paths
-- MAGIC
-- MAGIC Absolute paths are very tricky when trying to write portable code on an environment where every user has their own folder structure. The absolute path for one user will always be different from another user meaning if someone else clones this repository they would have to adjust any hardcoded file paths. This creates unnecessary code maintenance and introduces the risk of editing errors.
-- MAGIC
-- MAGIC As such, you would hope that DataBrick's `read_files()` SQL function would be able to hand a relative path. Unfortunately they do not. We will therefore have to build a filepath programmatically to make the code portable.
-- MAGIC
-- MAGIC To construct the file path we'll use a SQL variable, and the SQL function `CURRENT_USER()` to grab your email address.
-- MAGIC
-- MAGIC > Note that SQL variables are not supported below Databricks Runtime (DBR) 15.x so this means this solution could only work on DBR 15+. I am unaware of how this would be possible on a lower DBR.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dynamic filepath

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE air_bnb_filepath STRING;
SET VARIABLE air_bnb_filepath = CONCAT('"file:///Workspace/Users/',CURRENT_USER(),'/databricks_code_learn/resources/Airbnb_Open_Data.csv"');

SELECT air_bnb_filepath;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC The next most elegant solution would be to pass the dynamically created filepath as a variable to the `read_files()` function.
-- MAGIC
-- MAGIC > Note that the cell below fails and this is intentional. 

-- COMMAND ----------

SELECT * FROM read_files(air_bnb_filepath);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Unfortunately this doesn't work either. For some reason the `read_files()` function doesn't allow a variable as an argument and demands a hard coded file path.
-- MAGIC
-- MAGIC This enforces bad practice in code so we aren't going to do that, instead we're going to find a way that makes this code fully portable across users with no hardcoding of filepaths which would require code maintenance.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dynamic query
-- MAGIC
-- MAGIC The next most elegant solution would be to create the whole statement as a query and execute it, this would result in the `read_files()` function interpretting the filepath as a constant.
-- MAGIC
-- MAGIC You can see below that this works and presents us with a result set.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE read_file_statement string;

SET VARIABLE read_file_statement = CONCAT('SELECT * FROM read_files(',air_bnb_filepath,');');

EXECUTE IMMEDIATE read_file_statement;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC However, this result set isn't referencable from the Notebook session, so we have no way store it in a view or table from here.
-- MAGIC
-- MAGIC To get around this we will have to add a temporary view definition to the dynamic SQL statement we're creating.
-- MAGIC
-- MAGIC Thankfully this does work, and the temporary view is accessible from the spark session of the notebook.
-- MAGIC
-- MAGIC *At this point you may want to iterate on the temporary view to derive variables, or clean data before storing the result as a permenant table.*

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE read_file_statement string;

SET VARIABLE read_file_statement = CONCAT('CREATE OR REPLACE TEMPORARY VIEW air_bnb_sql  AS
SELECT * FROM read_files(',air_bnb_filepath,',
  format => "csv",
  header => true,
  mode => "PERMISSIVE"
);');

EXECUTE IMMEDIATE read_file_statement;

SELECT * FROM air_bnb_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Let's assume for now that the data is in the format we're wanting already and we just want to write it to a permenant table in our modelling area.
-- MAGIC
-- MAGIC > Note: The cell below fails and this is intentional.

-- COMMAND ----------

CREATE OR REPLACE TABLE air_bnb_sql AS 
SELECT * FROM air_bnb_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here we can see that it refuses to write to the table because there are invalid characters in some of the column names, specifically `host id`. The invalid character is the space '` `' in the name.
-- MAGIC
-- MAGIC There are two ways to handle this issue. Either we can iterate on the view and rename the columns as we go, or we can set the `TBLPROPERTIES()` item `delta.columnMapping.mode` to `name`.
-- MAGIC
-- MAGIC **Of the two strategies, the first is preferable** as it does enforce some standardisation to column naming across the platform (e.g. snake_case). Standardised naming conventions are useful as they make automation a lot easier if there is an expected format for a column name.
-- MAGIC
-- MAGIC However, this notebook will demonstrate both methods as a learning exercise as there may be times you need to import a CSV or JSON file as-is.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Data cleansing
-- MAGIC
-- MAGIC The first strategy is to clean the data and column names until they match the snake_case standard. To do this all you need to do is create a new temporary view that selects from `air_bnb_sql` and renames, derives or drops columns in the query.
-- MAGIC
-- MAGIC > Note that when using the `read_files()` function, as when importing data through the user interface, you will often have a `_rescued_data` column at the end. Hopefully this is populated entirely by nulls, in which case the column can just be dropped. If `_rescued_data` is populated it is essential that you investigate the contents of the column as it means the data has likely got corrupted.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW air_bnb_sql_clean AS
SELECT id
      ,NAME as name
      ,`host id` as host_id
      ,host_identity_verified
      ,`host name` as host_name
      ,neighbourhood
      ,lat
      ,long
      ,country
      ,`country code` as country_code
      ,instant_bookable
      ,cancellation_policy
      ,`room type` as room_type
      ,`Construction year` as construction_year
      ,price
      ,`service fee` as service_fee
      ,`minimum nights` as minimum_nights
      ,`number of reviews` as number_of_reviews
      ,`last review` as last_review
      ,`reviews per month` as reviews_per_month
      ,`review rate number` as review_rate_number
      ,`calculated host listings count` as calculated_host_listings_count
      ,`availability 365` as availability_365
      ,house_rules
      ,license
      --,_rescued_data - excluding this column from the select statement drops it
FROM air_bnb_sql;

SELECT * FROM air_bnb_sql_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now that all the column names are valid, we can simply select the view into a permenant table.
-- MAGIC
-- MAGIC > Note that because we set the data catalog and schema at the beginning of this notebook, there is no need to specify them here.

-- COMMAND ----------

CREATE OR REPLACE TABLE air_bnb_sql_cleaned AS 
SELECT * FROM air_bnb_sql_clean;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now SELECT the data from the permenant table.

-- COMMAND ----------

SELECT * FROM air_bnb_sql_cleaned;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Setting table properties to allow illegal characters
-- MAGIC
-- MAGIC As mentioned above this is not necessarily the preferred way of dealing with column names with 'illegal characters', but there are some circumstances where you may just want to import data as-is and do cleaning steps in other notebooks.
-- MAGIC
-- MAGIC As such we'll cover how to set the table you're creating to allow them by setting the `delta.columnMapping.mode` to `name`.
-- MAGIC
-- MAGIC This allows you to name table columns using characters that are not generally allowed by Databricks (such as spaces), so that users can directly ingest CSV or JSON data without the need to rename columns due to character constraints. For more information see [Rename and drop columns with Delta Lake column mapping](https://docs.databricks.com/en/delta/column-mapping.html).

-- COMMAND ----------

CREATE OR REPLACE TABLE air_bnb_sql_dirty TBLPROPERTIES('delta.columnMapping.mode' = 'name') AS
SELECT * FROM air_bnb_sql;

SELECT * FROM air_bnb_sql_dirty;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC When using this method you will need to enclose column names with illegal characters in backticks (` `` `) for Databricks to interpret them correctly (similar to SQL servers use of square brackets (`[]`)).

-- COMMAND ----------

SELECT `host name`, COUNT(*) AS N
FROM air_bnb_sql_dirty
GROUP BY `host name`
ORDER BY N DESC
LIMIT 15;
