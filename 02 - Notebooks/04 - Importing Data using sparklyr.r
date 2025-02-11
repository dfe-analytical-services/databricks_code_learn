# Databricks notebook source
# MAGIC %md
# MAGIC # Importing data
# MAGIC
# MAGIC In the previous notebook we imported data from a CSV using SQL. In this notebook we'll cover how to do the same thing but in the `R` language using the `sparklyr` library/package. 
# MAGIC
# MAGIC As in the previous notebook we will need to import data into Databricks so we can use it. We will once again use the [AirBnB opendata](https://www.kaggle.com/datasets/arianazmoudeh/airbnbopendata/data) set sourced from the [Kaggle website](https://www.kaggle.com/).
# MAGIC
# MAGIC > Note that as we're using the `sparklyr` package in R the default language for this Notebook in the top left corner is set to R.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set your catalog and schema
# MAGIC
# MAGIC We're going to want to make sure that everything is written to the schema created in earlier notebooks, so we'll want a widget again called schema. To make things easier in this workbook we'll also create a widget for the catalog.
# MAGIC
# MAGIC
# MAGIC As this Notebook is in R we'll use the R syntax for creating and referring to widgets.

# COMMAND ----------

#Creates a text box widget called catalog, with a default value of nothing
dbutils.widgets.text("catalog","")
#Creates a text box widget called schema, with a default value of nothing
dbutils.widgets.text("schema","")

# COMMAND ----------

# MAGIC %md
# MAGIC Fill in the `catalog` widget with the catalog for your team and the `schema` widget with the name you used in the previous notebooks. 
# MAGIC
# MAGIC Then run the next two chunks to create variables called `catalog` and `schema` in R with the value of the widgets.

# COMMAND ----------

catalog <- dbutils.widgets.get("catalog")
catalog

# COMMAND ----------

schema <- dbutils.widgets.get("schema")
schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Read in the CSV
# MAGIC
# MAGIC Now we'll read in the CSV data to R using the `spark_read_csv()` function from the `sparklyr` package.

# COMMAND ----------

# MAGIC %md
# MAGIC ### `sparklyr`
# MAGIC
# MAGIC Now to begin importing it to the unity catalog we'll need to load the `sparklyr` package and create a connection variable. We'll also load the `stringr` package to help with string manipulation.

# COMMAND ----------

library(stringr)
library(sparklyr)

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Absolute filepaths (again)
# MAGIC
# MAGIC As with the SQL `read_files()` function `sparklyr`'s `spark_read_csv()` function also requires an absolute filepath rather than a relative one.
# MAGIC
# MAGIC To make this code portable between users we will need to dynamically construct the filepath using code. Thankfully, in `R` this is a lot simpler to do due to the `getwd()` function, which on DBR 15+ returns the file path of the current notebook.
# MAGIC
# MAGIC As the `CSV` we're wanting to import is in another folder within the repository root directory, we'll need to split the filepath on the slashes (`/`) and then remove any subfolders after the 'databricks_code_learn' directory.
# MAGIC
# MAGIC First we split the file path.

# COMMAND ----------

path_components <- str_split(getwd(), "/")[[1]]
path_components

# COMMAND ----------

# MAGIC %md
# MAGIC Then determine which position of the filepath matches the 'databricks_code_learn' title (if for some reason you named this repository something else you will need to adjust the code accordingly).

# COMMAND ----------

repo_root_dir_position <- which(path_components == "databricks_code_learn")
repo_root_dir_position

# COMMAND ----------

# MAGIC %md
# MAGIC Now we can remove all the subdirectories after the repository root folder and stitch the items back together as a single string, separated by `/`.

# COMMAND ----------


repo_root_dir <- paste0(path_components[1:repo_root_dir_position], collapse = "/")
repo_root_dir

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to add on the subfolder(s) and filename of the CSV we're wanting to read, along with pre-pending the path with `file://` to tell Databricks what kind of input to expect.

# COMMAND ----------

csv_path <- paste0("file://",repo_root_dir,"/resources/Airbnb_Open_Data.csv")
csv_path

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading in the CSV data
# MAGIC
# MAGIC We can now use the `spark_read_csv()` function to read in the contents of the CSV.
# MAGIC
# MAGIC > *Note that below to see the dataframe we have to use the `display()` function. If you try to use `View()` as you would in RStudio you will get an error.*

# COMMAND ----------

air_bnb_r <- spark_read_csv(sc, #connection variable defined above
                            name = "air_bnb_r", #name to give the dataset within spark, can be referenced in SQL statements
                            path = csv_path) #absolute path to the CSV to import along with a file protocol preceding it
display(air_bnb_r)

# COMMAND ----------

# MAGIC %md
# MAGIC You can see in the spark dataframe above that R/`sparklyr` have automatically sanitised the column names by replacing any illegal characters with an underscore (`_`).
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Storing the CSV data in the unity catalog
# MAGIC
# MAGIC The only thing left to do now is to write the table out to a permenant table. The `spark_write_table` function takes a spark dataframe (`air_bnb_r`) and the name you want to give to the table.
# MAGIC
# MAGIC > Note as we set the catalog and schema earlier in the notebook there is no need to specify them in the `name` parameter. If you haven't set the catalog and/or schema earlier you would need to use the 3 part *catalog_name*.*schema_name*.*table_name* convention.

# COMMAND ----------

spark_write_table(x = air_bnb_r, name = "air_bnb_r")

# COMMAND ----------

# MAGIC %md
# MAGIC Finally we can confirm that it worked by selecting from the permenant table.

# COMMAND ----------

display(sdf_sql(sc, "SELECT * FROM air_bnb_r"))
