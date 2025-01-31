--As mentioned in the readme of this folder, this file is not saved as a DataBricks query, and due to this it is not bound by the limitations of the 'SQL Editor'
--If you click on the 'Connect' button to the top right you'll see that you're able to run this file with either a 'SQL Warehouse' or a 'Personal Cluster'. For this section, choose a personal cluster instead of a SQL warehouse

--You'll also notice that to the left of this file there are icons for a folder, and some shapes (triangle with a circle and square on the bottom). Clicking on these brings up a pane that allows you to explore the folder structure of the project, or explore the unity catalog. This is actually more convinient than the SQL editor UI as there you are unable to browse the project.

--You may have noticed there are no line numbers here, but these can be switched on by going to 'View' > 'Line numbers'.

--We're now going to demonstrate the difference between SQL editor, and running this file as a script that has the extention `.sql`.
--In the SQL editor we demonstrated that the whole query had to be ran as a single query or it would 'forget' temporary views defined earlier.
--Here, where we are just working off a SQL file, this doesn't happen. The file has it's own 'session' and you can run everything line by line. This is true whether you use a personal cluster, or a SQL warehouse.

--Run the following two lines in isolation by highlighting them and pressing Ctrl+Shift+Return/Enter
USE CATALOG catalog_40_copper_analyst_training;
USE imdb;


--Now run lines 18 to 34 in isolation by highlighting them and pressing Ctrl+Shift+Return/Enter
CREATE OR REPLACE TEMPORARY VIEW the_moth_and_the_flame AS
SELECT a.tconst as title_id
      ,a.originalTitle as movie_title
      ,a.startYear as release_year
      ,a.genres 
      ,b.category as role_type
      ,b.characters as characters_played
      ,c.primaryName as crew_member_name
      ,c.birthYear as crew_member_birth_year
      ,c.deathYear as crew_member_death_year
      , c.primaryProfession as crew_member_primary_profession
FROM title_basics a
LEFT JOIN title_principals b
  ON a.tconst = b.tconst
LEFT JOIN name_basics c
  ON b.nconst = c.nconst
WHERE a.tconst = 'tt0005773';


--Now run line 38 by highlighting it and pressing Ctrl+Shift+Return/Enter
SELECT * FROM the_moth_and_the_flame;

--In the SQL Editor this would have resulted in an error because the temporary view 'the_moth_and_the_flame' didn't exist within the session. Here however, everything works as we are used to from SSMS (with syntax differences not withstanding).

-- The output window that will appears below is still limited showing one result set however.

SELECT * FROM the_moth_and_the_flame LIMIT 5;
SELECT * FROM the_moth_and_the_flame LIMIT 1;

--In conclusion:
---The SQL Editor is not the most like-for-like experience for using SQL in Databricks despite it's familiar looking layout.
--While you can use this method instead (creating a generic file and giving it a `.sql` extention), Notebooks are still preferable to this format as they allow formatted text and images that can better document what your code is trying to do, and allow you to display numerous table results in a single set of code. We will learn about them in the next section.