-- Databricks notebook source
-- MAGIC %md
-- MAGIC # SQL
-- MAGIC
-- MAGIC In the previous section we learnt how to use Notebooks to document our work using markdown. Now we'll start using some data and populate our modelling areas with something to use for the other sections.
-- MAGIC
-- MAGIC For this notebook to function correctly ensure that you have attached it to a personal cluster and not a SQL warehouse.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Defining the catalog
-- MAGIC
-- MAGIC First we'll define what data catalog we're wanting to bring data into and set that at the beginning of the Notebook. 
-- MAGIC
-- MAGIC Run the `CREATE WIDGET` chunk below and you should see a text input box called 'catalog' appear at the top of this page. Enter the name of your team's modelling area data catalog. You should have been granted access to a catalog as part of the onboarding process.
-- MAGIC
-- MAGIC To see which catalogs you have access to you can press the data catalog icon (![Data catalog icon](../images/notebooks-catalog-icon.png)) at the left hand side of this page. Your team's modelling area will most likely start with `catalog_40_copper`.
-- MAGIC
-- MAGIC If you hover over the name of a catalog an ellipsis will appear to the righthand side. If you click on it you will be given the option to 'Copy catalog path' which will allow you to easily paste the name into the widget above.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT '';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now run the chunk below to tell this notebook to run the rest of the commands against that catalog.

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Creating a schema to work in using widgets
-- MAGIC
-- MAGIC To ensure that we're each working in our own area and not overwriting the same data, we'll create a schema each in our modelling areas before pulling data through. To do this we'll
-- MAGIC utilise a Notebook widget to set the schema. 
-- MAGIC
-- MAGIC The block below creates a widget called `schema`, and once the chunk is ran this will appear as a text box at the top of the Notebook. Whatever you put in this box can be referred to in the code using the syntax `:schema`.

-- COMMAND ----------

CREATE WIDGET TEXT schema DEFAULT '';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC *Type your name into the widget and then run the chunk below to create a schema named after you.*

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS IDENTIFIER(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You'll notice above that the `:catalog` and `:schema` widget reference is wrapped in an `IDENTIFIER()` function. This is required when using SQL and passing a parameter as the name of a catalog object (catalog / schema / table / view).
-- MAGIC
-- MAGIC After running the chunk above use the catalog explorer pane to the left to check that your schema has been created. 
-- MAGIC
-- MAGIC ![Data catalog explorer](../images/notebooks-catalog.png)
-- MAGIC
-- MAGIC You may need to press the refresh button if this window was already open, this is highlighted below. 
-- MAGIC
-- MAGIC ![Catalog explorer refresh button](../images/notebooks-catalog-refresh.png)
-- MAGIC
-- MAGIC We'll also tell this Notebook that we're wanting to `USE` this schema.

-- COMMAND ----------

USE IDENTIFIER(:schema);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Importing data
-- MAGIC
-- MAGIC Now everyone has their own schema, let's import some data to it. 
-- MAGIC
-- MAGIC In this Notebook we'll focus on importing data from another data catalog. Specifically, we'll import data to your modelling area from the analyst training data catalog `catalog_40_copper_analyst_training`.
-- MAGIC
-- MAGIC We'll grab some game reviews from the `steam` schema. We'll filter it down slightly to only reviews where the reviewer had played 20 hours or over of the game, and those which 100 or more readers tagged as 'helpful'.
-- MAGIC
-- MAGIC First let's just create a temporary view to hold that data.
-- MAGIC
-- MAGIC As we're working in our current catalog and schema in this notebook, we'll reference the `steam_game_reviews` table in the analyst training catalog using it's full three level name: `catalog.schema.table`.

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW reviews AS
SELECT * 
FROM catalog_40_copper_analyst_training.steam.steam_game_reviews
WHERE hours_played >= 20 and helpful >= 100;

SELECT COUNT(*) AS N FROM reviews;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now let's take a quick look at the data. Note that to see the first 1000 rows the syntax is different in DataBricks than SSMS.
-- MAGIC
-- MAGIC In SSMS we would write
-- MAGIC
-- MAGIC `SELECT TOP 1000 * FROM table`
-- MAGIC
-- MAGIC Where as in Databricks we use
-- MAGIC
-- MAGIC `SELECT * FROM table LIMIT 1000`

-- COMMAND ----------

SELECT * FROM reviews LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Above we can see the results in a table in the chunk output. We can use this to browse any data returned from a `SELECT` query.
-- MAGIC
-- MAGIC Now we've taken a look at the data, let's store it in our modelling area.
-- MAGIC
-- MAGIC Remember we set the catalog and schema to use at the top of this Notebook, so when we create a table it will be created in the schema we created in our catalog.

-- COMMAND ----------

CREATE OR REPLACE TABLE steam_reviews AS
SELECT * FROM reviews;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Verify the data has been loaded correctly
-- MAGIC
-- MAGIC Once again, check that the table has successfully been created using the Catalog pane to the left. Remember that you may need to click the 'Refresh' button in the top right of the pane to see any changes.
-- MAGIC
-- MAGIC ![Catalog explorer refresh button](../images/notebooks-catalog-refresh.png)
