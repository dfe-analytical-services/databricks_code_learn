# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Scripting a workflow in Databricks
# MAGIC
# MAGIC Workflows can be constructed through the Databricks Workflows user interface (UI), however for large or complex workflows the UI can be a time consuming way to build a workflow. In these scenarios it is quicker and more inline with RAP principles to script your workflow.
# MAGIC
# MAGIC For a pipeline to be built there must be scripts, queries or notebooks available to read by Databricks, either located in your workspace, or in a Git repository.
# MAGIC
# MAGIC For this example we will use the notebooks stored in `resources/workflow notebooks` as the tasks, and this notebook will use code (R) to create a workflow from them. We’ll also set it up to notify us by email when the workflow successfully completes.
# MAGIC
# MAGIC The workflow constructed is the same as in the previous notebook ('02 - Building a Workflow in the UI') and it is recommended that you complete that exercise before this one.
# MAGIC
# MAGIC > **Note:** The default language of this notebook has been set to `R` to prevent us having to use the `%r` _magic command_ in every cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Terminology
# MAGIC
# MAGIC The terms 'workflow' and 'jobs' are used pretty interchangably in the UI of Databricks. In the code of the `databricks` R package they are more frequently referred to as 'jobs'.
# MAGIC
# MAGIC For consistency with previous exercises this notebook uses the term 'workflow' but the meaning of a workflow or job is the same.
# MAGIC
# MAGIC A 'task' is a component of a workflow/job, and is always referred to as a 'task'.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Required packages
# MAGIC
# MAGIC In the chunk below we load the `tidyverse` package, then install the `devtools` package and load it. 
# MAGIC
# MAGIC We then use `devtools::install_github()` function to install the `databricks` package, then load it.
# MAGIC
# MAGIC We'll also require the `sparklyr` package and a spark connection (`sc`) to retrieve data from Databricks and bring it into R.

# COMMAND ----------

library(tidyverse)

install.packages("devtools")
library(devtools)

install_github("databrickslabs/databricks-sdk-r")
library(databricks)

library(sparklyr)

sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Authentication
# MAGIC
# MAGIC The Databricks API that is used by the `databricks` package requires authentication to connect in the form of a Personal Authentication Token (PAT). We do not want to put this in the code, as it would be the equivalent of putting your username and password into a script. First however we'll need to generate one from the Databricks UI.
# MAGIC
# MAGIC ### Databricks access token
# MAGIC
# MAGIC Personal Authentication Token (PAT)s are a unique code that is generated to let Databricks know who is accessing it from the outside. It functions as a password and so must not be shared with anyone. 
# MAGIC
# MAGIC To generate one:
# MAGIC  1. In the top righthand corner of the Databricks page click the coloured circle with the first initial of your name
# MAGIC  2. Followed by 'Settings'
# MAGIC  3. Click 'Developer' in the 'Settings' sidebar
# MAGIC  4. Click 'Manage' next to 'Access tokens'
# MAGIC  
# MAGIC  ![Generate a PAT](../images/workflow-script-pat.png)
# MAGIC
# MAGIC  5. Click the ‘Generate new token’ button
# MAGIC  6. Name the token, then click ‘Generate’
# MAGIC  7. Copy and paste the token somewhere safe and _private_
# MAGIC
# MAGIC > **It is very important that you immediately copy the access token that you are given, as you will not be able to see it through Databricks again.** If you lose this access token before saving it somewhere then you must generate a new access token to replace it.
# MAGIC
# MAGIC > **Note:** PATs will only last as long as the value for the ‘Lifetime (days)’ field. After this period the token will expire, and you will need to create a new one to re-authenticate. Access tokens also expire if they are unused after 90 days. For this reason, we recommend setting the Lifetime value to be 90 days or less.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we've generated a PAT, run the cell below to create the widget at the top of the page. Once the widget is there paste in your personal access token into the text box.

# COMMAND ----------

dbutils.widgets.text("api_token", "")

# COMMAND ----------

# MAGIC %md
# MAGIC We're also going to need a widget for the name of your modelling area (`catalog`) and the name of the group that owns the catalog (`owner_group`). These will be passed through to the workflow and so we need them accessible to the code.  
# MAGIC
# MAGIC If you're unsure on where to find your catalog name of the catalog owner's group name, refer to the instructions in the previous notebook '02 - Building a Workflow in the UI' under the 'Workflow settings' section.
# MAGIC
# MAGIC Run the chunk below and then add in the values for the `catalog` and `owner_group` widgets.

# COMMAND ----------

dbutils.widgets.text("catalog","")
dbutils.widgets.text("owner_group","")

# COMMAND ----------

# MAGIC %md
# MAGIC Finally, we'll add a widget for the Databricks production URL called `host`. This isn't needed from a technical point of view, we could just hard-code it into the script as it is unlikely to change. 
# MAGIC
# MAGIC However, as this repository isn't access controlled and is available to the public on GitHub it would be more secure to keep the web address of the Departments data analytics platform out of the code. It also means if the URL for Databricks ever did change there would be less code maintenance.
# MAGIC
# MAGIC Run the chunk below and then populate it with the URL of this current page up until (and including) the first `/`, and your token.

# COMMAND ----------

dbutils.widgets.text("host","")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to Databricks through it's API
# MAGIC
# MAGIC We can now connect to the API through the `databricks` package using the `databricks::DatabricksClient()` function. It requires the `host` which and your token which we just defined above. We’ll store the result in a variable called `client` as we need to pass this to the other functions in the `databricks` library, the same way we would have to pass an ODBC connection variable to a database query.

# COMMAND ----------

host <- dbutils.widgets.get("host")
api_token <- dbutils.widgets.get("api_token")

client <- databricks::DatabricksClient(host = host, token = api_token)

# COMMAND ----------

# MAGIC %md
# MAGIC We can then use the `databricks::clustersList()` function to fetch a list of the clusters, which we can view using `display()`. To ensure we get the correct cluster though we'll also need our username(/email address) which we can get from Databricks using the `sparklyr` connection we set up earlier.

# COMMAND ----------

user_name <- sdf_sql(sc, "SELECT CURRENT_USER();") %>% collect() %>% as.character()

clusters <- databricks::clustersList(client) %>% filter(single_user_name == user_name)

display(
  clusters %>% 
    select(cluster_id, single_user_name, creator_user_name, cluster_name, spark_version, cluster_cores, cluster_memory_mb)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The `databricks::clustersList()` function will return any clusters that you have permission to see. Sometimes a user may have access to more than a single cluster, so we'll order the clusters by `spark_version` (descending) and take only the first result, then store it's id in a variable called `cluster_id`.
# MAGIC
# MAGIC > NOTE: The data returned by the function is hierarchical, and a single ‘column’ may contain several other columns. As the `display()` function renders a table, you’ll have to select only columns that `display()` knows how to show. Generally, the columns that are at the left-most position when you run `str(clusters)` (shows the structure).

# COMMAND ----------

cluster_id <- clusters %>% 
                select(cluster_id, single_user_name, spark_version) %>% 
                arrange(desc(spark_version)) %>%
                filter(row_number() == 1) %>% 
                pull(cluster_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define workflow and tasks
# MAGIC
# MAGIC Now we have our connection to Databricks API and our `cluster_id` we’ll start by setting some parameters for the workflow. 
# MAGIC
# MAGIC Firstly we’ll need a `job_name`, and the paths to the Notebooks we’re wanting to include in the workflow. We’ll also need to create a unique `task_key` for each of the Notebook tasks we’re going to set up.
# MAGIC
# MAGIC As the notebooks that we want to turn into tasks are stored in this repository we will need to provide relative paths (to the root folder of the repository). If we were referring to notebooks stored in our workspace but not a repository we would need to use absolute paths.

# COMMAND ----------

job_name <- "test job"

create_schema_key <- "create_schema"
create_schema_path <- "resources/workflow notebooks/01 - Create schema if not exist"

create_table_key <- "create_table"
create_table_path <- "resources/workflow notebooks/02 - Create table if not exist"

insert_data_key <- "insert_data"
insert_data_path <- "resources/workflow notebooks/03 - Insert data"

select_from_table_key <- "select_from_table"
select_from_table_path <- "resources/workflow notebooks/04 - Select from table"

# COMMAND ----------

# MAGIC %md
# MAGIC We can then define the tasks as lists. There are many options available available for setting when creating a task, a full list of which can be found in the tasks section of the [job API documentation](https://docs.databricks.com/api/workspace/jobs). When reading this documentation any parameter that is marked as an object needs to be passed as a list (`list()`) in R, and anything marked as an array should be passed as a vector (`c()`).
# MAGIC
# MAGIC For the first task we’ll give it the first `task_key` we created above, and tell it to run on our existing cluster by passing the ID of our cluster to `existing_cluster_id`, we’ll then specify that it is a `notebook_task` and pass that a list with the `notebook_path` and the `source` which we will set to `GIT` (as opposed to 'Workspace').
# MAGIC
# MAGIC

# COMMAND ----------

create_schema_task <- list(
  task_key = create_schema_key,
  existing_cluster_id = cluster_id,
  notebook_task = list(
    notebook_path = create_schema_path,
    source = "GIT"
  )
)

str(create_schema_task)

# COMMAND ----------

# MAGIC %md
# MAGIC For the second task we will do the same, switching to the relevant `task_key` and `notebook_path`. In addition, we’ll also add a `depends_on` clause with the previous `task_key` (passed in a list), and specify it is only to `run_if` `ALL_SUCCESS`. This means that the second task won’t begin processing unless all of the tasks it `depends_on` have completed successfully.

# COMMAND ----------

create_table_task <- list(
  task_key = create_table_key,
  existing_cluster_id = cluster_id,
  notebook_task = list(
                    notebook_path = create_table_path,
                    source = "GIT"
                  ),
  depends_on = list(task_key = create_schema_key),
  run_if = "ALL_SUCCESS"
)

str(create_table_task)

# COMMAND ----------

# MAGIC %md
# MAGIC We'll do the same for the third task, this time switching the key, and path to the relevant variables defined above, and the dependencies to those of the `create_table_task`.

# COMMAND ----------

insert_data_task <- list(
  task_key = insert_data_key,
  existing_cluster_id = cluster_id,
  notebook_task = list(
                    notebook_path = insert_data_path,
                    source = "GIT"
                  ),
  depends_on = list(task_key = create_table_key),
  run_if = "ALL_SUCCESS"
)

str(insert_data_task)

# COMMAND ----------

# MAGIC %md
# MAGIC Finally we'll do the same with the last task.

# COMMAND ----------

select_from_table_task <- list(
  task_key = select_from_table_key,
  existing_cluster_id = cluster_id,
  notebook_task = list(
                    notebook_path = select_from_table_path,
                    source = "GIT"
                  ),
  depends_on = list(task_key = insert_data_key),
  run_if = "ALL_SUCCESS"
)

str(select_from_table_task)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Git details
# MAGIC
# MAGIC Since we are running this workflow from a git repository we will need to pass the git details to the workflow definition. Specifically the URL of the repository, the Git provider, and the branch of the repository we are wanting to build the workflow from.

# COMMAND ----------

git_source_list <- list(
          git_url = "https://github.com/dfe-analytical-services/databricks_code_learn",
          git_provider = "gitHub",
          git_branch = "main"
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters
# MAGIC
# MAGIC Finally, the tasks in the workflow have parameters, specifically the `catalog` to write data into and the `owner_group` of that catalog to ensure the permissions are set correctly for anyone in the group to modify any schemas/tables/views created.
# MAGIC
# MAGIC We could add parameters to each task individually, and if the values of those parameters was different for different tasks we would need to do this. Since the `catalog` and `owner_group` values are the same for each of our tasks we can simply add the parameters at a workflow level. These will be accessible to all the tasks within the workflow.
# MAGIC
# MAGIC The list of parameters is structured as a list which contains within it a list for each parameter with the values `name` and `default`. We could add our parameter values here as the `default` and therefore by default the parameters would always contain those values unless they were overridden by parameters passed at the time the workflow was run.
# MAGIC
# MAGIC Sometimes it is preferable for a workflow to fail than to make changes to database objects by accident. Having default values can run the risk of making it too easy to accidentally start a workflow without parameters and change the wrong database. This is a design decision that should be considered when building any automated workflow. 
# MAGIC
# MAGIC For the purpose of this tutorial, we'll leave the default values blank so that the workflow fails if there are no parameters passed on the basis that it removes any chance of an accidental run that could risk overwriting previous tables/data.

# COMMAND ----------

parameters_list <- list(
  list(name = 'catalog', default = ''),
  list(name = 'owner_group', default = '')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a workflow
# MAGIC
# MAGIC Now we have both of our tasks defined we can create the workflow using the `databricks::jobCreate()` function. We pass it the `client` as the first argument, then the workflow `name` we defined. The `tasks` are passed as a list which contains each of the task lists we built above.
# MAGIC We’ll also tell it to send us `email_notifications` by passing a list with an `on_success` value of email addresses. As our `user_name` variable defined above is the same as our email address we can re-use this variable here.
# MAGIC
# MAGIC The `jobsCreate()` function returns the ID of the workflow we just created, so we will want to store the response in a variable called `workflow` so we can refer to it later.

# COMMAND ----------

  workflow <- jobsCreate(client,
      name = job_name,
      git_source = git_source_list,#defined above
      tasks = list(
                  create_schema_task, #defined above
                  create_table_task, #defined above
                  insert_data_task, #defined above
                  select_from_table_task), #defined above
      parameters = parameters_list, #defined above
      email_notifications = list(
                              on_success = c(user_name)
                              )
)

workflow

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC > ### _Lists of lists_
# MAGIC >
# MAGIC > _A `list()` in R is used to contain any number and type of data, including other `list()`s. This makes it excellent for storing hierarchical data in one place, however it can get quite confusing quite quickly._
# MAGIC >
# MAGIC > _Sometimes it’s easier to break these `lists()` up into pieces by defining them seperately, as we did above by defining the task lists separately then passing them to the `tasks` argument in the `jobsCreate()` function._
# MAGIC >
# MAGIC > _This often makes it easier to think about and construct, but certainly makes it easier to read. Consider the code below which does exactly the same thing as the code above, but is just written all at once. It's much harder to tell what's going on when trying to absorb all that information at once._
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC     workflow <- jobsCreate(client,
# MAGIC     name = job_name,
# MAGIC     git_source = list(
# MAGIC         git_url = "https://github.com/dfe-analytical-services/databricks_code_learn",
# MAGIC         git_provider = "gitHub",
# MAGIC         git_branch = "main"
# MAGIC       ),
# MAGIC     tasks = list(
# MAGIC               list(
# MAGIC                   task_key = create_schema_key,
# MAGIC                   existing_cluster_id = cluster_id,
# MAGIC                   notebook_task = list(
# MAGIC                     notebook_path = create_schema_path,
# MAGIC                     source = "GIT"
# MAGIC                   )
# MAGIC                 ), #create_schema
# MAGIC                 list(
# MAGIC                   task_key = create_table_key,
# MAGIC                   existing_cluster_id = cluster_id,
# MAGIC                   notebook_task = list(
# MAGIC                                     notebook_path = create_table_path,
# MAGIC                                     source = "GIT"
# MAGIC                                   ),
# MAGIC                   depends_on = list(task_key = create_schema_key),
# MAGIC                   run_if = "ALL_SUCCESS"
# MAGIC                 ), #create_table
# MAGIC                 list(
# MAGIC                   task_key = insert_data_key,
# MAGIC                   existing_cluster_id = cluster_id,
# MAGIC                   notebook_task = list(
# MAGIC                                     notebook_path = insert_data_path,
# MAGIC                                     source = "GIT"
# MAGIC                                   ),
# MAGIC                   depends_on = list(task_key = create_table_key),
# MAGIC                   run_if = "ALL_SUCCESS"
# MAGIC                 ), #insert_data
# MAGIC                 list(
# MAGIC                   task_key = select_from_table_key,
# MAGIC                   existing_cluster_id = cluster_id,
# MAGIC                   notebook_task = list(
# MAGIC                                     notebook_path = select_from_table_path,
# MAGIC                                     source = "GIT"
# MAGIC                                   ),
# MAGIC                   depends_on = list(task_key = insert_data_key),
# MAGIC                   run_if = "ALL_SUCCESS"
# MAGIC                 )
# MAGIC               ), #select_from_table
# MAGIC     parameters = list(
# MAGIC                   list(name = 'catalog', default = ''),
# MAGIC                   list(name = 'owner_group', default = '')
# MAGIC                 ),
# MAGIC     email_notifications = list(
# MAGIC                             on_success = c(user_name)
# MAGIC                             )
# MAGIC     )

# COMMAND ----------

# MAGIC %md
# MAGIC > _We can see here that the code is getting very long, and is also more difficult to see which options relate to which list. If it weren’t for being diligent with indentation here we’d have to resort to counting brackets to see what belonged where. This is especially problematic if you accidentally delete a bracket and need to work out where it was meant to go._

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Run the workflow through code
# MAGIC
# MAGIC We can now get the ID of the job that was created and tell the API to run the workflow. In a new code chunk we’ll store the `job_id` from the `workflow` variable above. We’ll then use the `databricks::jobsRunNow()` function to tell it to run the workflow we just created by passing it the `job_id` we just stored. 
# MAGIC
# MAGIC A workflow may be run many times and each of these runs is given an ID. To target a specific run of a workflow in code we'll need to store the `job_run_id` returned by the `databricks::jobsRunNow()` function.

# COMMAND ----------

job_id <- workflow$job_id

job_run <- jobsRunNow(client, 
                      job_id = job_id)

job_run_id <- job_run$run_id
job_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Create a link for the workflow run
# MAGIC
# MAGIC We will now use this to create links to the workflow and the specific run of the workflow we just set off.
# MAGIC
# MAGIC In a new code cell, define a `job_link` by `paste0()`ing the `host` variable we passed to the `databricks::DatabricksClient()` function earlier, followed by `"job/"` followed by the `job_id` defined above.
# MAGIC We can then create a `job_run_link` by `paste0()`ing together the `job_link` followed by `"/runs/"` then the `job_run_id` from the previous step.
# MAGIC We can then output the `job_link` as text at the bottom of the cell.

# COMMAND ----------

job_link <- paste0(host,"jobs/",job_id)
job_run_link <- paste0(job_link,"/runs/", job_run_id)
job_run_link

# COMMAND ----------

# MAGIC %md
# MAGIC Now click on the link above and check that it links to a workflow run before returning to the tutorial.
# MAGIC
# MAGIC Once you've clicked the link you should see a graph, but the workflow will have failed. The reason it failed is because we forgot to pass any parameters to the it, and since we left the default values of the parameters blank the first code chunk that referred to them threw an error. 
# MAGIC
# MAGIC This is what we wanted because not only did it prevent any accidental changes being made it also failed immediately allowing us to investigate the cause. This is preferable to accidentally making changes we didn't want but looking like a successful run, which could mean we don't even notice anything has gone wrong until later in the day when we're trying to access data we just overwrote.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Running with parameters
# MAGIC
# MAGIC Now let's run the workflow with parameters and (hopefully) get a successful run out of it.
# MAGIC
# MAGIC We'll now take the values of the `catalog` and `owner_group` from the widgets we created earlier and pass them as parameters to the `databricks::jobsRunNow()` function.
# MAGIC
# MAGIC Here the argument for passing parameters is `job_parameters`, and the list is in the structure of a single list with each item being `parameter_name = 'parameter value'`. 

# COMMAND ----------

catalog <- dbutils.widgets.get("catalog")
owner_group <- dbutils.widgets.get("owner_group")

job_run <- jobsRunNow(client, 
                      job_id = job_id,
                      job_parameters = list(
                          catalog = catalog,
                          owner_group = owner_group
                        ))

job_run_id <- job_run$run_id
job_run_id

# COMMAND ----------

# MAGIC %md
# MAGIC We'll now output another link and check how successful the run of the workflow was.

# COMMAND ----------

job_link <- paste0(host,"jobs/",job_id)
job_run_link <- paste0(job_link,"/runs/", job_run_id)
job_run_link

# COMMAND ----------

# MAGIC %md
# MAGIC All being well you should now see a graph of tasks turning green in sequence. Once the final task has completed check your modelling area catalog and you should see a schema called `shared`, containing a table called `shared_table_runs`.
# MAGIC
# MAGIC Within that table there should be one or more rows depending on how many times this workflow has been run in your modelling area.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleaning up
# MAGIC
# MAGIC You’ve now created a workflow with code, and each time you re-run this notebook another workflow with the same name will be created. As this is a tutorial which most analysts may have to follow at some point, there's a reasonable chance that we could end up with several ‘test jobs’ cluttering up the workflow page.
# MAGIC
# MAGIC To avoid that we could use the `databricks::jobsDelete()` function (as below) to clean up after ourselves. All that we need to do is pass the function the `client`, and `job_id` variables from above.
# MAGIC
# MAGIC However, let's look at another way to do it.

# COMMAND ----------

#jobsDelete(client, job_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bulk Clean up
# MAGIC  
# MAGIC If you have been running and re-running bits of this code iteratively, there’s a good chance you already have several instances of ‘test job’ listed under your name.
# MAGIC
# MAGIC ![Multiple test jobs](../images/workflow-script-multiple-jobs.png)
# MAGIC
# MAGIC If this is the case we’ll want to clean up each of these, ideally without having to manually click through the UI process for each one.
# MAGIC
# MAGIC To do this, firstly call the `databricks::jobsList()` function, passing it the `client` variable, and specifying the `name` of the workflows you want to list. Then filter the list to just the workflowss with a `creator_user_name` of your email address (which is still stored in the `user_name` variable defined above). 
# MAGIC
# MAGIC > Note: It's highly unlikely that you would get any workflows that weren't yours due to the default Databricks permissions model, but there is the possibility that people may have shared access to their workflows with you, so to guard against accidentally interfering with someone else's work it doesn't hurt to put an explicit safeguard in.

# COMMAND ----------

my_jobs <- jobsList(client, name = "test job") 

display(my_jobs %>% select(job_id, creator_user_name, run_as_user_name, created_time))

# COMMAND ----------

# MAGIC %md
# MAGIC We can now loop through the individual `job_id`s contained in `my_jobs` and use the `databricks::jobsDelete()` function to remove them all at once, programmatically.
# MAGIC
# MAGIC If you do only have one instance of 'test job' this loop will only run once, and will have the same effect as just deleting the workflow as above, but if you have multiple occurances this loop will run as many times as needed until all of the 'test jobs' under your name have been removed.

# COMMAND ----------

for(job_id in my_jobs$job_id){
      jobsDelete(client, job_id)
}
