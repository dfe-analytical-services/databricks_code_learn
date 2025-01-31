# SQL Editor

The SQL editor is the Databricks equivalent of SQL Server Management Studio. It looks relatively familiar with a data explorer pane on the left, a script window in the top right and a results window in the bottom right. It can be found on the left hand panel of the Databricks page under the 'SQL' section.

There are several key differences from SSMS to bear in mind though, and we'll demonstrate this with the query files in this folder.

## Queries

Any file that is created as a SQL query within Databricks will automatically open in the 'SQL Editor' page of the Databricks interface, and so will be subject to the rules that apply to it. In addition Databricks Query files are not supported by Git and so cannot be versioned in a repository. 

For this and other reasons which we will explore in this section it is probably easier to avoid using the SQL Editor or Databricks queries in general. The same things can be accomplished using other methods which are more user friendly. These will be covered in subsequent sections.

## SQL Editor rules

Two that are particularly significant are:

- Compute for the SQL editor is restricted to 'SQL Warehouses', which are a SQL only type of compute. You are not able to use your personal cluster within the SQL editor. 
- The way that a 'session' is handled is quite different from what we are used to in SSMS, and so it may not be the best tool for iterative or interactive script writing. We'll explore this in query `03 - Temporary views and sessions`.

## Getting started

Right click on 'SQL Editor' and 'Open in a new window'.

For the 01, 02 and 03 `.sql` files in this folder:
- Copy the text and paste it into the SQL editor window
- Read the comments and follow the instructions 

For 04, follow the instructions and run it in the file itself.

The reason you can't just click on the files and they open in the 'SQL Editor' is because of the issue with `Git` and version control mentioned above.

## Recommendations

The SQL Editor may have the most familiar looking interface when compared to SSMS, but there are some limitations and quirks of it's functionality that make it less than ideal. Many of these issues can be avoided when using generic files with a `.sql` extension or Notebooks instead of 'queries'. Both are more versatile and hold the session in memory so you can refer back to objects created in earlier queries. 

This section was included because in the interest of familiarity and education it is worth having an awareness of the SQL editor. This section has been placed first so we can quickly get it out of the way and move on to a friendlier format.