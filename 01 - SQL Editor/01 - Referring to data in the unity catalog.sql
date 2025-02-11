--A query will open by default in the DataBricks SQL Editor. This has been designed to look similar to SQL Server Management Studio (SSMS) but there are a few differences which will be covered throughout these lesson.

--Copy this code into the Databricks SQL editor and follow along the text below

--Querying data
---Data Catalogs
--The first issue will be locating the table in the unity catalog. 
--There are a few aspects of the UI that will help us with this. 
--Firstly, the panel on the left of this window allows you to browse the data catalogs you have access to
--Secondly, the catalog and schema buttons just above here (likely they say 'hive_metastore' and 'default') allow you to set which catalog this query should run in.

--However, when drafting a query it is always useful to put the catalog in the query itself so that other users that may inherit the script will know where it is supposed to be run.
--There are two ways to do this:
----1. Use the full name of the table when referring to it in your query (e.g. 'catalog_name.schema_name.table_name')
----2. Explicitly set the catalog at the beginning of the query (e.g. 'USE CATALOG catalog_name;')

--1. Using the full name of the table - run this line in isolation by highlighting it and pressing Ctrl+Shift+Return/Enter
SELECT * FROM catalog_40_copper_analyst_training.covid.covid_data LIMIT 10;

--There are two undesirable issues with method 1:
----1. It's more typing, which leads to longer code
----2. It's less portable. If you wanted to migrate the data and query to a different catalog, or make a development version of it you would need to change the name of the table in every line it was referenced. 
----   which takes more effort and also significantly increases the chance of introducing an editing error.

--2. Setting the catalog at the beginning of the query - run these lines in isolation by highlighting them and pressing Ctrl+Shift+Return/Enter
USE CATALOG catalog_40_copper_analyst_training;
SELECT * FROM covid.covid_data LIMIT 10;

--This syntax allows you to only specify the catalog once and then refer to it only by schema_name.table_name
--You can still reference tables in other databases but you will need to specify that database name using the full catalog_name.schema_name.table_name syntax.

---Schemas
--You can also do the same with schemas. This allows you to refer to tables only by their name - run these lines in isolation by highlighting them and pressing Ctrl+Shift+Return/Enter
USE CATALOG catalog_40_copper_analyst_training;
USE covid;
SELECT * FROM covid_data LIMIT 10;