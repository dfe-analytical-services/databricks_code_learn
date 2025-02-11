-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Notebooks
-- MAGIC
-- MAGIC One of the key benefits of using notebooks is that they have both *code chunks* and *markdown chunks* which can be used to document your process as you go. This text is stored in a _markdown chunk_. If you double click this text you'll find yourself able to edit it.
-- MAGIC
-- MAGIC There are **several** advantages to this over using code comments:
-- MAGIC - Formatting of text makes it far easier for a person to read
-- MAGIC - You can include images and diagrams in markdown chunks to convey complexity far more easily than trying to describe it in text
-- MAGIC - Markdown headers can be used to clearly separate out different sections of the notebook
-- MAGIC - Markdown headers also allow you to navigate the document more easily
-- MAGIC
-- MAGIC Notebooks are compatible with SQL warehouses and personal clusters, and unlike the 'SQL Editor' it retains previous temporary view definitions in memory so that you can refer to them later on without having to run the entirety of the code.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Getting started
-- MAGIC
-- MAGIC Firstly, we'll take a quick look around the 'Notebook' page and get familiar with the layout.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Page contents

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Title, repositories and default language
-- MAGIC
-- MAGIC In the top left, we have the Notebook title, a `Git` button with the name of the branch we are on (if we are in a repository folder, if not this won't appear) and a default language button.
-- MAGIC
-- MAGIC ![Notebook title, repo and default language](../images/notebooks-nav-1.png)
-- MAGIC
-- MAGIC Clicking on any of these will allow you to edit them.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Run, compute, schedule and share
-- MAGIC
-- MAGIC In the top right we have four buttons; 'Run all', 'Connect', 'Schedule' and 'Share'.
-- MAGIC
-- MAGIC ![Run, compute, schedule and share buttons](../images/notebooks-nav-2.png)
-- MAGIC
-- MAGIC The 'Run all' button is relatively self-explanatory. It runs all the code chunks in the notebook in sequential order from top to bottom.
-- MAGIC
-- MAGIC The 'Connect' button allows you to attach a *compute* resource to the notebook. This will either be a personal cluster or a SQL warehouse. Once a compute resource has been selected the button will display the name of that resource. For the purposes of these exercises we will be using our personal clusters.
-- MAGIC
-- MAGIC The schedule button allows you to create a job to run the notebook on a schedule. This may be useful for automation of simple repetitive tasks. For more complex tasks or processes it may be more appropriate to use a workflow which we will cover in later sections.
-- MAGIC
-- MAGIC Finally, the 'Share' button allows you to grant access to this notebook to colleagues, and set what type of permission they can have (can they view, edit, run, or manage it). This feature should be used with extreme caution though, as if you grant permission for someone to edit your workbook any changes they make will automatically be saved and may break your code.
-- MAGIC
-- MAGIC > *Rather than sharing through Databricks, best practice would be to store your notebooks in a Git repository (GitHub, or Azure DevOps) which can be cloned by colleagues and managed through version control.*

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Navigation pane
-- MAGIC
-- MAGIC To the left of this notebook there are the following icons:
-- MAGIC
-- MAGIC ![Workspace icons](../images/notebooks-workspace-icons.png)
-- MAGIC
-- MAGIC From top to bottom these are:
-- MAGIC - Table of contents (see 'Headers' section below)
-- MAGIC - Workspace browser (files and folders)
-- MAGIC - Data Catalog browser (data catalogs, schemas, tables, views and volumes)
-- MAGIC - AI assistant (AI powered chat bot which can attempt to fix your code.)
-- MAGIC
-- MAGIC Clicking any of these icons will bring up a side pane with the relevant content in.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Main window
-- MAGIC
-- MAGIC In the main window is where you write your markdown and code to create the workbook itself. The main body of a notebook is made up of two types of content:
-- MAGIC
-- MAGIC - Markdown blocks for text, images, lists, links, etc.
-- MAGIC - Code blocks for running SQL, R, python or scala
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Markdown chunks
-- MAGIC
-- MAGIC If you hover between or below a *chunk* two buttons will appear allowing you to add either a markdown block or a code block (`+ Code` or `+ Text`). If you click markdown (`+ Text`) you will get a text box with formatting options.
-- MAGIC
-- MAGIC If for whatever reason you wanted to change a code chunk to a markdown block, you can do this by adding `%md` to the top of the block (for Markdown). Likewise you can change a markdown block to a codeblock of any language using `%r`, `%sql`, `%python` or `%scala`. These are referred to as *magic commands* and they tell Databricks how to interpret the content of each block.

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC
-- MAGIC ### Headers
-- MAGIC
-- MAGIC Headers in the markdown make the document more readable, but also come with the benefit of making them easier to navigate.
-- MAGIC
-- MAGIC If you click on the top icon, a 'Table of contents' pane opens up and shows you a hierarchical list of headers. You can navigate to any section of the notebook just by clicking on the header.
-- MAGIC
-- MAGIC ![Table of contents](../images/notebooks-toc.png)
-- MAGIC
-- MAGIC One thing to bear in mind is that only the first header of a chunk is listed in the table of contents. To make the notebook easiest to navigate each new heading or sub-heading should be created in a new markdown chunk.
-- MAGIC
-- MAGIC This also comes with the benefit of being able to easily drag and drop each section to re-order them if required.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Formatting
-- MAGIC
-- MAGIC With markdown you can use plain text to define the format of the text or content to be displayed. Below is a (non-exhaustive) table of how to achieve formatting with markdown syntax.
-- MAGIC
-- MAGIC | Element         | Markdown Syntax |
-- MAGIC |-----------------|-----------------|
-- MAGIC | Heading         | `# H1` `## H2` `### H3` `#### H4` `##### H5` `###### H6` |
-- MAGIC | Block quote     | `> blockquote` |
-- MAGIC | Bold            | `**bold**` |
-- MAGIC | Italic          | `*italicized*` |
-- MAGIC | Strikethrough   | `~~strikethrough~~` |
-- MAGIC | Horizontal Rule | `---` |
-- MAGIC | Code            | ``` `code` ``` |
-- MAGIC | Link            | `[text](https://www.example.com)` |
-- MAGIC | Image           | `![alt text](image.jpg)`|
-- MAGIC | Ordered List    | `1. First items` <br> `2. Second Item` <br> `3. Third Item` |
-- MAGIC | Unordered List  | `- First items` <br> `- Second Item` <br> `- Third Item` |
-- MAGIC | Code Block      | ```` ``` ```` <br> `code block` <br> ```` ``` ````|
-- MAGIC | Table           |<code> &#124; col &#124; col &#124; col &#124; </code> <br> <code> &#124;---&#124;---&#124;---&#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br> <code> &#124; val &#124; val &#124; val &#124; </code> <br>|
-- MAGIC
-- MAGIC
-- MAGIC If you double click this markdown chunk the content will switch from formatted to editable markdown text. Try it now, then click outside of the chunk to exit again.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Code chunks
-- MAGIC
-- MAGIC Although properly formatted documentation is **very cool** (and I'm sure that's exactly what you were all thinking right now), it wouldn't be much use without the code to document.
-- MAGIC
-- MAGIC Like with a markdown chunk you can create a code chunk by clicking the buttons that appear when you hover over the gap between chunks and choosing `+ Code`. You can also convert any chunk to a code block of a given language using the appropriate *magic command* at the top of the chunk.
-- MAGIC
-- MAGIC Once you run a code chunk, any output will be displayed at the bottom of the chunk.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Default language
-- MAGIC
-- MAGIC As mentioned above, each notebook has a default language, and any chunks that have no language specified by a *magic command* will be assumed to be the default language of the notebook.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Magic commands
-- MAGIC
-- MAGIC Magic commands allow you to execute code in multiple languages in the same notebook. 
-- MAGIC
-- MAGIC | Magic command | Description |
-- MAGIC | --- | --- |
-- MAGIC |`%md`| Create a markdown chunk for formatted content|
-- MAGIC |`%r`, `%sql`, `%python`, `%scala`| Run code in a cell on a given programming language|
-- MAGIC |`%run`| Run script or notebook|
-- MAGIC
-- MAGIC There are some limitations to being able to use several languages in the same notebook though. Each language is isolated from the others and so a variable defined in `python` cannot be accessed in `R` and vice versa.
-- MAGIC
-- MAGIC Data can be shared between the two languages using tables and views by writing out the data in language A and reading it in in language B. Realistically it's probably quite rare that there's a strong use-case for multi-language notebooks, but it doesn't hurt to know how to do it.
-- MAGIC
-- MAGIC It's worth noting that when using a 'SQL warehouse' type of compute you are only able to use SQL so switching to R/python/scala will cause an error.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Examples
-- MAGIC
-- MAGIC The default language of this notebook is SQL, so I don't need to write any magic commands at the top to execute a SQL query or statement.

-- COMMAND ----------

DECLARE VARIABLE hello_world string;
SET VARIABLE hello_world = 'Hello world!';

SELECT hello_world;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC To switch to R we have to use the magic command `%r` at the top.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC hello_world <- "Hello world!"
-- MAGIC
-- MAGIC print(hello_world)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For python: `%python`

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC hello_world = "Hello world!"
-- MAGIC
-- MAGIC print(hello_world)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC Finally, for scala: `%scala`

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC def hello_world = "Hello world!"
-- MAGIC println(hello_world)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Widgets
-- MAGIC
-- MAGIC Notebooks can also have [widgets](https://docs.databricks.com/en/notebooks/widgets.html), which are one way of parameterising the code within a notebook. They appear as a text box, or drop down box at the top of a notebook and their contents can be referenced in your code. You can create a widget using any language, and when it is created you must set a default value. 
-- MAGIC
-- MAGIC Widgets are very useful when creating notebooks or processes that may need to be re-run with different values. They can be thought of as similar to arguments in function calls (in R, SQL and python) or stored procedures (SQL).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Below we'll demonstrate the different types of widgets, syntax for SQL and R are provided.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### SQL
-- MAGIC
-- MAGIC We'll first demonstrate using SQL syntax.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Text input
-- MAGIC
-- MAGIC The simplest form of widget is simply a free-text field where you can type in what you want it's value to be. Here we'll create a text widget and refer to it in the notebook to pull out it's value.

-- COMMAND ----------

CREATE WIDGET TEXT sql_text DEFAULT 'This is a SQL text widget';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now refer to the widget and retrieve it's value.
-- MAGIC
-- MAGIC Run the cell below, then change the contents of the `sql_text` widget and re-run it.
-- MAGIC
-- MAGIC Note that to refer to the widgets value we using the syntax `:widget_name`.

-- COMMAND ----------

SELECT :sql_text;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dropdowns
-- MAGIC
-- MAGIC There may be situations where you want to parameterise something, but only have a limited number of options the user can change it to. For this purpose a dropdown widget is useful.

-- COMMAND ----------

CREATE WIDGET DROPDOWN sql_dropdown DEFAULT 'A' CHOICES SELECT * FROM (VALUES ("A"), ("B"),("C"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Click on the newly created `sql_dropdown` widget and you'll see the list of options available for a user to select. Choose one of these and then run the chunk below to see your code.

-- COMMAND ----------

SELECT :sql_dropdown;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Combobox
-- MAGIC
-- MAGIC For situations where you have many options and want a user to be able to search them using free-text you can use a combobox widget. A combobox will allow a user to search for and select a single value from a list of pre-defined values.
-- MAGIC
-- MAGIC

-- COMMAND ----------

CREATE WIDGET COMBOBOX sql_combo DEFAULT "Aadvark" CHOICES SELECT * FROM (VALUES ("Aadvark"), ("Arrow"), ("Apple"),("Banana"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The chunk above created a drop down widget where you can type and search for one of the options. Delete the word 'Aadvark' from the `sql_combo` widget and you'll see the other available options pop up to be seleted. Choose one and then run the chunk below to see the selected value.

-- COMMAND ----------

SELECT :sql_combo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Multiselect
-- MAGIC
-- MAGIC There may also be situations where you want to allow the user to select several values from a list of predefined values. For this purpose, a multiselect widget can be used.

-- COMMAND ----------

CREATE WIDGET MULTISELECT sql_multi DEFAULT "Aadvark" CHOICES SELECT * FROM (VALUES ("Aadvark"), ("Arrow"), ("Apple"),("Banana"));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC In the widget `sql_multi` select a couple of options before running the next cell. 
-- MAGIC
-- MAGIC As with a combobox you can type the value you want to select, and/or select it from the options in the drop down.

-- COMMAND ----------

SELECT :sql_multi;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Here you can see that each selected option is returned, separated by a comma (`,`).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using widgets as catalog / schema / table identifiers
-- MAGIC
-- MAGIC Widgets are quite powerful in that they allow you to parameterise anything in your notebook, including names of catalogs, schemas, and tables or views.
-- MAGIC
-- MAGIC Run the chunk below to create a widget that allows you to select the name of any table in the `steam` schemas of the `catalog_40_copper_longitudinal_ilr` catalog.

-- COMMAND ----------

CREATE WIDGET DROPDOWN sql_table_name DEFAULT 'steam_game_reviews' CHOICES SELECT * FROM (SELECT DISTINCT table_name FROM catalog_40_copper_analyst_training.information_schema.tables WHERE table_schema IN ('steam'));

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Select one of the options from the `sql_table_name` widget drop down and then run the cell below.
-- MAGIC
-- MAGIC Below we create a SQL variable called `table_reference` to hold the full path of the table selected in the widget above, then we can use that table reference in a query by wrapping it in the `IDENTIFIER()` function.
-- MAGIC
-- MAGIC After running the cell below choose another option from the drop down and re-run to retrieve the data for a different table.

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE table_reference string;
SET VARIABLE table_reference = CONCAT('catalog_40_copper_analyst_training.steam.',:sql_table_name);

SELECT * FROM IDENTIFIER(table_reference) LIMIT 1000;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Removing widgets
-- MAGIC
-- MAGIC If you need to remove a widget, you can do so using the following syntax within a SQL code chunk:

-- COMMAND ----------

REMOVE WIDGET sql_combo;

-- COMMAND ----------

REMOVE WIDGET sql_dropdown;
REMOVE WIDGET sql_multi;
REMOVE WIDGET sql_text;
REMOVE WIDGET sql_table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### R
-- MAGIC
-- MAGIC Widgets can also be created and referenced in R code, although the syntax is quite different. To create and reference widgets in the R language you need to reference the `dbutils` module which is a library of functions that allows you to interact with Databricks functionality through R/python/scala.  
-- MAGIC
-- MAGIC The `dbutils` module is always available within the Databricks environment, but (like widgets) is not transferable to code outside of Databricks e.g. RStudio.
-- MAGIC
-- MAGIC For more information on the `dbutils` module you can visit the [Databricks Utilities (dbutils) reference](https://docs.databricks.com/en/dev-tools/databricks-utils.html) page in the Databricks documentation.
-- MAGIC
-- MAGIC > Note: The default language for this workbook is `SQL`, so when using `R` commands note the `%r` magic command at the top of each code chunk. This tells Databricks to interpret the code using `R` rather than `SQL`.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Textbox
-- MAGIC
-- MAGIC As with the SQL syntax you will need to give the widget a unique name, and a default value when it is created.

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC dbutils.widgets.text("r_text", "This is an R text widget");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can store the value of a widget in a variable and use it throughout your code.

-- COMMAND ----------

-- MAGIC %r 
-- MAGIC r_text <- dbutils.widgets.get("r_text")
-- MAGIC r_text

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Dropdowns
-- MAGIC
-- MAGIC The first two arguments when creating widgets are always the name and the default value. For the choices, these must be supplied as a list of options as a third argument.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC
-- MAGIC dbutils.widgets.dropdown("r_dropdown", "A", list("A","B","C"))

-- COMMAND ----------

-- MAGIC %r
-- MAGIC dbutils.widgets.get("r_dropdown")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Combobox

-- COMMAND ----------

-- MAGIC %r
-- MAGIC dbutils.widgets.combobox("r_combo", "Aadvark", list("Aadvark", "Arrow", "Apple","Banana"))

-- COMMAND ----------

-- MAGIC %r
-- MAGIC
-- MAGIC dbutils.widgets.get("r_combo")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Multiselect
-- MAGIC

-- COMMAND ----------

-- MAGIC %r
-- MAGIC dbutils.widgets.multiselect("r_multi", "Aadvark", list("Aadvark", "Arrow", "Apple","Banana"))

-- COMMAND ----------

-- MAGIC %r
-- MAGIC dbutils.widgets.get("r_multi")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Using widgets as catalog / schema / table identifiers
-- MAGIC
-- MAGIC In R there are a few extra steps to creating a widget that is powered by values in a catalog/database. First we'll have to create a connection using the `sparklyr` library.
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %r
-- MAGIC library(sparklyr)
-- MAGIC
-- MAGIC sc <- sparklyr::spark_connect(method = "databricks")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can then write a query to get the table names from the `information_schema` of the `catalog_40_copper_analyst_training` catalog, where the schema is `steam`.
-- MAGIC
-- MAGIC > Note: The `sparklyr` package deals with data queries by storing a reference to the data rather than the data itself (similar to a view in SQL). So when you get the results of a query using `sdf_sql()` the dataframe that is returned doesn't actually contain the data until we use the `collect()` function which brings the data into memory.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC get_schema_table_names <- "SELECT DISTINCT table_name FROM catalog_40_copper_analyst_training.information_schema.tables WHERE table_schema = 'steam';"
-- MAGIC
-- MAGIC table_names <- sdf_sql(sc, get_schema_table_names) %>% collect()
-- MAGIC display(table_names)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Now we will need to convert the `table_names` dataframe we fetched above into a list to populate the default and choices options for the widget.
-- MAGIC
-- MAGIC The `as.list()` function turns the `table_names` dataframe column `table_name` into a list which we can pass to the function to create the dropdown widget.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC table_names_list <- table_names$table_name %>% as.list()
-- MAGIC table_names_list

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now pass the first item of `table_names_list` as the default value for the widget, and the whole list as the options.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC dbutils.widgets.dropdown("r_table_name", # name of the widget
-- MAGIC                         table_names_list[[1]], # default option
-- MAGIC                         table_names_list) # choices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We can now retrieve the value of the widget and construct a query to get the tables data before displaying it.
-- MAGIC
-- MAGIC > Note: Here we don't need to use the `IDENTIFIER()` function because we are constructing the query string before we send it to Databricks.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC table_choice <- dbutils.widgets.get("r_table_name")
-- MAGIC
-- MAGIC sql_query <- paste0("SELECT * FROM catalog_40_copper_analyst_training.steam.",table_choice," LIMIT 1000;")
-- MAGIC
-- MAGIC table_contents <- sdf_sql(sc, sql_query)
-- MAGIC
-- MAGIC display(table_contents)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Removing widgets
-- MAGIC
-- MAGIC Similar to SQL you can remove a single widget by name using the following command.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC
-- MAGIC dbutils.widgets.remove("r_text");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In addition, in R there is also a command to remove all widgets at once.

-- COMMAND ----------

-- MAGIC %r
-- MAGIC
-- MAGIC dbutils.widgets.removeAll()
