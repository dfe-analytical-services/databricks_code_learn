--Copy this code into the Databricks SQL editor and follow along the text below

--Before running the lines of code below, write a quick statement about what you expect to see in the results window on the line below:
--A: 
USE CATALOG catalog_40_copper_analyst_training;
USE SCHEMA steam;

SELECT * FROM games_description LIMIT 10;
SELECT * FROM games_ranking LIMIT 10;

--What did you actually get?
--A:




--You can see now that the 'SQL Editor' in Databricks will only return one result set per run, and it is always the last result in the statement.
--Here we only got results for the `games_ranking` table, the results for the `games_description` table are not displayed. This is another key difference between SQL Editor and SQL Server Management Studio (SSMS).