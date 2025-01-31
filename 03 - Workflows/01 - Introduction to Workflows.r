# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Workflows
# MAGIC
# MAGIC Workflows allow you to build complex data pipelines by chaining together multiple scripts, queries, notebooks and logic. 
# MAGIC
# MAGIC They can be used to build Reproducible Analytical Pipelines (RAP) that can be re-run with different parameters and have all inputs and outputs audited automatically. 
# MAGIC
# MAGIC In addition, workflows can be set to run on a schedule or a trigger you define, and can be setup to notify people when it succeeds (/ fails).
# MAGIC
# MAGIC Other recommended uses of workflows are any data modelling tasks such as cleaning your source data and collating it into a more analytically friendly format in your modelling area.
# MAGIC
# MAGIC The Workflows page can be accessed from the left-hand side bar. 
# MAGIC
# MAGIC ![Workflows sidebar](../images/workflow-menu.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tasks and dependencies
# MAGIC
# MAGIC Each step in a workflow is referred to as a task and each task can have dependencies, which are other tasks that must have run before it. 
# MAGIC
# MAGIC Tasks are usually scripts, or Notebooks which can be held either in your Databricks Workspace, or (preferably) in a Git repository either on Github or Azure DevOps. 
# MAGIC
# MAGIC Below is an example of a workflow with several tasks. The last task will only run once all other tasks have completed as it is dependant on them having completed.
# MAGIC
# MAGIC ![Workflow with tasks and dependencies](../images/workflow-task-chart.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auditing
# MAGIC
# MAGIC Each time a workflow is run, Databricks audits:
# MAGIC - any input parameters
# MAGIC - all outputs
# MAGIC - the success and failure of each task
# MAGIC - when it was run and who by
# MAGIC
# MAGIC ![Workflow auditing](../images/workflow-audit.png)
# MAGIC
# MAGIC This makes workflows a very powerful debugging tool as you can refer back to results from previous runs. This means that if your pipeline fails you can review the notebook(s) that failed and troubleshoot the issue.
# MAGIC
# MAGIC ![Workflow task failure](../images/workflow-notebook.png)
# MAGIC
# MAGIC Workflows that fail also allow you to repair the workflow once you have found and fixed the issue. This prevents having to re-run the whole pipeline from scratch and allows it to pick up from the point where it failed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coded workflows
# MAGIC
# MAGIC Another useful aspect of workflows is that they can be defined and ran using code through the [DataBricks Jobs API](https://docs.databricks.com/api/workspace/jobs).
# MAGIC
# MAGIC There is an R library which has been created to interface with the DataBricks API, meaning that you can script jobs in R using the [DataBricks SDK for R package](https://docs.databricks.com/en/dev-tools/sdk-r.html) using lists instead of JSON.
# MAGIC
# MAGIC This is covered in more detail in the notebook '03- Scripting a workflow'.
