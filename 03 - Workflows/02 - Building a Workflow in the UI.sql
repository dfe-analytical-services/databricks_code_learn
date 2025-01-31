-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Building a workflow using the Databricks Workflows user interface
-- MAGIC
-- MAGIC For this section we will use the notebooks stored in `resources/workflow notebooks` to build a workflow using the user interface.
-- MAGIC
-- MAGIC The workflow will take a catalog as a parameter. Within that catalog it will
-- MAGIC
-- MAGIC  - Create a schema called 'shared' if it doesn't already exist (01 - Create schema if not exist)
-- MAGIC  - Create a table called 'shared_table_runs' if it doesn't already exist (02 - Create table if not exist)
-- MAGIC  - Insert a row into the table of the current user and the time the row was inserted (03 - Insert data)
-- MAGIC  - Select all the rows from the table and display them (04 - Select from table)
-- MAGIC
-- MAGIC Before following this exercise it is recommended that you look over the Notebooks in the `resources/workflow notebooks` folder and familiarise yourself with the syntax for each step.
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create a new workflow
-- MAGIC
-- MAGIC Right click on the 'Workflows' option in the left hand menu and click 'Open in a new window'.
-- MAGIC
-- MAGIC In the workflows page click the blue 'Create job' button on the right hand side.
-- MAGIC
-- MAGIC ![Workflow page, create job](../images/workflow-create-job.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You will then be presented with a page titled ‘New Job [timestamp]’ which you can edit to give the workflow a meaningful name.
-- MAGIC
-- MAGIC For the purposes of this exercise title the job 'test_job_[your name]'.
-- MAGIC
-- MAGIC ![Job name](../images/workflows-new-job.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Workflow settings
-- MAGIC
-- MAGIC > The ‘Job Details’ pane on the right allows you to configure workflow level schedules and triggers, parameters which will be accessible to all tasks in the workflow, email notifications for successful / failed runs, and permissions on who can access and run the workflow. You can also add tags and descriptions to your workflow to help you keep track of them.
-- MAGIC
-- MAGIC This workflow will create a schema in your modelling area titled `shared`, however due to the way permissions work on Databricks the person to create the schema will become it's owner, meaning no one else can use it. For this (and probably most) purpose we need anyone with access to the catalog to have the ability to use the schema, so the workflow will address this too by setting the owner to the *group* that manages the database.
-- MAGIC
-- MAGIC #### Finding your group name
-- MAGIC
-- MAGIC To do this we'll need to `ALTER` the `OWNER TO` the name of the group that owns the catalog. This will be done by one of the notebooks in the workflow, but we need to add it as a parameter so that notebook is able to set the permissions to the correct group. 
-- MAGIC
-- MAGIC To find the group name navigate to your catalog in the ['Catalog Explorer'](https://adb-5037484389568426.6.azuredatabricks.net/explore/data?o=5037484389568426) and then click the 'Permissions' tab. There should be a table with the headings 'Principal', 'Privilege' and 'Object'. You'll want to copy the 'Principle' name with "RWC" (Read / Write / Create) in it and add it as the value for the 'owner_group' job parameter (see below).
-- MAGIC
-- MAGIC ![Catalog group name](../images/workflow-catalog-group.png)
-- MAGIC
-- MAGIC ### Job Parameters
-- MAGIC
-- MAGIC Click the 'Edit parameters' button under 'Job parameters' then add a parameter with a 'Key' of 'catalog' and a 'Value' of the name of your teams data catalog. Add a second parameter with a 'Key' of 'owner_group', and a value of the RWC (Read / Write / Create) group that manages the catalog.
-- MAGIC
-- MAGIC ![Edit parameters](../images/workflow-parameter.png)
-- MAGIC
-- MAGIC ### Job Notifications
-- MAGIC
-- MAGIC Next, let's also setup a notification to tell us when the workflow has finished. Click the 'Edit notification' button under the 'Job notifications' header then click the blue 'Add notification' button.
-- MAGIC
-- MAGIC In the 'Select a destination' box choose 'Email address' and type in your email address ensure that the 'Success' and 'Failure' are ticked then click 'Save'. 
-- MAGIC
-- MAGIC ![Edit notifications](../images/workflow-notification.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Defining tasks
-- MAGIC
-- MAGIC Now we'll need to define the tasks in the main window. 
-- MAGIC
-- MAGIC It's generally a good idea to give the name of the task a descriptive one, so for this workflow we'll just reuse the names of the notebooks for the task names. Task names can't have spaces in though so we'll use underscores (`_`) instead.
-- MAGIC
-- MAGIC ### Create schema if not exist
-- MAGIC
-- MAGIC Name the first task create_schema_if_not_exist. 
-- MAGIC
-- MAGIC You can leave the 'Type' as notebook, but change the 'Source' to 'Git provider', then click the blue 'Edit' link to the right.
-- MAGIC
-- MAGIC In the 'Git information' box put the following settings and click 'Confirm':
-- MAGIC
-- MAGIC - Git repository URL: https://dfe-gov-uk.visualstudio.com/official-statistics-production/_git/databricks_code_learn
-- MAGIC - Git provider: Azure DevOps Services
-- MAGIC - Git reference: main (branch)
-- MAGIC
-- MAGIC This ensures that the workflow runs the code from this Git repository on the 'main' branch.
-- MAGIC
-- MAGIC ![Workflow Git settings](../images/workflow-git.png)
-- MAGIC
-- MAGIC Once you've set your git settings set the 'Path' to `resources/workflow notebooks/01 - Create schema if not exist`.
-- MAGIC
-- MAGIC Change the compute to your personal cluster.
-- MAGIC
-- MAGIC Click Create task.
-- MAGIC
-- MAGIC ![Workflow task pane](../images/workflow-task-pane.png)
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table if not exist
-- MAGIC
-- MAGIC Click the blue '+ Add task' button and choose 'Notebook' to add the second task. 
-- MAGIC
-- MAGIC Notice that it has assumed that this task 'Depends on' the previous task. In this case this is what we want, although in a more complex workflow we may want to change the dependent task, or add others. This can be done through the 'Depends on' field.
-- MAGIC
-- MAGIC For this task the settings should be the same as the previous task with the exception of the 'Task name' and 'Path' which should be as follows:
-- MAGIC
-- MAGIC - Task name: create_table_if_not_exists
-- MAGIC - Path: resources/workflow notebooks/02 - Create table if not exist
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Insert Data
-- MAGIC
-- MAGIC Click the blue '+ Add task' button and choose 'Notebook' to add a new task. 
-- MAGIC
-- MAGIC For this task the setting should be the same as the previous tasks with the exception of the 'Task name' and 'Path' which should be as follows:
-- MAGIC
-- MAGIC - Task name: insert_data
-- MAGIC - Path: resources/workflow notebooks/03 - Insert data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Select from table
-- MAGIC
-- MAGIC Click the blue '+ Add task' button and choose 'Notebook' to add a new task. 
-- MAGIC
-- MAGIC For this task the setting should be the same as the previous tasks with the exception of the 'Task name' and 'Path' which should be as follows:
-- MAGIC
-- MAGIC - Task name: select_from_table
-- MAGIC - Path: resources/workflow notebooks/04 - Select from table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Review workflow
-- MAGIC
-- MAGIC Now you've added all the notebooks as tasks your workflow should look similar to the one below:
-- MAGIC
-- MAGIC ![Task flow for demonstration workflow](../images/workflow-task-flow.png)
-- MAGIC
-- MAGIC Assuming it does, click the blue 'Run now' button in the top right hand corner of the page. This will trigger a new run of the job and you'll likely get a pop up in the top right corner with a link to 'View run'. Click the link to watch the workflow run.
-- MAGIC
-- MAGIC > Note: To run the workflow your cluster must be running. If it isn't running when you click the 'Run now' button your cluster will begin to start up automatically however this may take a few minutes so will delay the running of the tasks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Review the results
-- MAGIC
-- MAGIC Once the workflow has completed and (hopefully) succeeded running all the tasks click on the 'select_from_table' task box to view the notebook it ran and it's outputs.
-- MAGIC
-- MAGIC This should output a table with the names and run-times of everyone that has built and ran this workflow. 
-- MAGIC
-- MAGIC ![Workflow results showing all people who have ran the workflow](../images/workflow-results.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
