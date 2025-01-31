--Copy this code into the Databricks SQL editor and follow along the text below

--Another key difference between Microsoft SQL (MS SQL) and DataBricks SQL is the use of temporary tables/views
--In MS SQL you would create a temporary table, whereas in Databricks you would create a temporary view

--Highlight the lines below 7 - 23 below and press Ctrl+Shift+Return/Enter to run the code
USE CATALOG catalog_40_copper_analyst_training;
USE imdb;

CREATE OR REPLACE TEMPORARY VIEW movie AS
SELECT tconst
      ,titleType
      ,primaryTitle
      ,originalTitle
      ,isAdult
      ,startYear
      ,endYear
      ,runtimeMinutes
      ,genres
FROM title_basics
Where tconst = "tt0005773";

SELECT * FROM movie;

-- You can create as many temporary views as your process needs to transform the data, and use them as if they were tables in subsequent queries
--To demonstrate this, highlight lines 7 to 52 and press Ctrl+Shift+Return/Enter to run the code


CREATE OR REPLACE TEMPORARY VIEW cast_and_crew AS
select * FROM title_principals WHERE tconst = "tt0005773" order by tconst;

CREATE OR REPLACE TEMPORARY VIEW cast_and_crew_names AS
SELECT * FROM name_basics WHERE nconst IN (SELECT nconst FROM cast_and_crew);

CREATE OR REPLACE TEMPORARY VIEW summary AS
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
FROM movie a
LEFT JOIN cast_and_crew b
  ON a.tconst = b.tconst
LEFT JOIN cast_and_crew_names c
  ON b.nconst = c.nconst;

SELECT * FROM summary;

--Now we'll use the `summary` temporary view to explore another difference in how the Databricks SQL editor functions from SSMS
--Run the following line in isolation by highlighting it and pressing Ctrl+Shift+Return/Enter
SELECT * FROM summary ORDER BY release_year;

--Here we got an error saying it can't find the 'summary' table/view, even though we just defined the view above and ran that code.
--This is due to the way SQL Editor handles 'sessions' differently to SSMS In SSMS a 'session' holds all of the information for you while you have the editor open so you can run things line by line
--Databricks SQL editor treats whatever line(s) are ran as their own separate session, so when we ran the above line on it's own there was no view called summary available to it, because that 
--was defined and ran in the previous session.

--This difference makes it a bit more difficult to use interactively for development than SSMS. Thankfully, these issues can be overcome by using notebooks instead, which we will explore in the next section