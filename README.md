# Introduction 

This repository is designed to have work-along exercises that explore some of the functionality of Databricks. It also attempts to demonstrate some differences between using SQL and R through SSMS / RStudio, and using them on DataBricks. We will also explore the different working spaces, storage spaces, etc. that are available for use on DataBricks.

This repository is designed to be run in the 'Production' environment of DataBricks within the Department for Education. During the onboarding process you should have been sent an email from the ADAPT team with a link to the Production environment.

_**Note:** This won't actually say 'Production'. It will instead appear as a code of letters and numbers, but for the purposes of this tutorial it is presented as 'Production'._

![](images/databricks-readme-environment.png)

There are 4 environments in total; Production, Pre-production, Test, and Dev. The associated codes are in the table below. The Production environment is the only environment on which sensitive data should be kept. As such the majority of analytical work will take place on this environment.

Each environment looks the same apart from the name in the top right corner, however they are not connected to each other and data on one environment is not accessible from others. You may not have access to all of these environments, that is not a problem as long as you have access to the Production environment.

As the name in the top right is the only way to identify which environment you are on it can be helpful to create bookmarks in your browser titled with the respective environment name.

# Pre-requisites

To use this repository you will need:
- Access to the Databricks Production environment
- Within Databricks production you will need
   - A modelling area or catalog for you or your team with read/write/create permission
   - A SQL warehouse that is accessible for you or your team


These should all be taken care of by the ADA team when you are onboarded, however if you notice anything missing you should contact the ADA team to resolve it using [ADAPTTeam@education.gov.uk](mailto:ADAPTTeam@education.gov.uk).

## Recommendations

Databricks has a few new concepts that it helps to understand a bit about before use. These are covered briefly in the [Databricks fundamentals](https://dfe-analytical-services.github.io/analysts-guide/ADA/databricks_fundamentals.html) page in the [Analysts' Guide](https://dfe-analytical-services.github.io/analysts-guide/). Reading through this will help you navigate the platform while working through this repository.

For the exercises that are in the folder `01 - SQL Editor` the use of dual monitors is recommended as there is some copying and pasting between different Databricks pages. This is not required but will make the experience easier.

# Getting Started

## Create compute on Databricks 
To begin the exercises you will first need to setup up a personal cluster from the 'Compute' page in the left hand sidebar of the Databricks web page.

In order to access data and run code you need to set up a compute resource. A compute resource provides processing power and memory to pick up and manipulate the data. There are several types of compute resource available which are displayed in the table below. For this repositority you will need a 'SQL warehouse' (should be provided on onboarding) and a 'Personal cluster'.

Instructions on setting up a personal cluster are provided under the table below.

| Compute type | Description |
| ------------ | ----------- |
| SQL Warehouse | Can be used by multiple users, can use SQL commands only; best to use if you’re only querying data from Databricks using SQL |
| Personal cluster | Only used by a single user, supports R, SQL, python and scala; best to use if you require languages other than SQL. |
| Shared cluster | Can be used by multiple users, supports SQL, python and scala but not R |

1. Click 'Compute'
2. Press the 'Create with DfE Personal Compute' button
3. Switch the 'Node type' to 'Large 28GB 8-core node'
4. Click the 'Create compute' button at the bottom. This will start your cluster. Your cluster may take several minutes to start up.

Databricks charges based on the usage of compute, so if you finish work on Databricks it is good practice to terminate it when you're done to save money for the Department. As a pre-caution against running up unnecessary charges your personal cluster is set to terminate after 1 hour of inactivity. 

Once you have a cluster started you can begin working through the numbered folders sequentially. Each folder will have a README or numbered notebooks describing what the exercise is.


## Clone this repository to Databricks

In order to access this repository from the Databricks environment you'll need to set up access to DevOps through Databricks, then clone this repo into your workspace. Instructions on how to do this can be found in the [Analyst Guide](https://dfe-analytical-services.github.io/analysts-guide/) article [Use Databricks with Git](https://dfe-analytical-services.github.io/analysts-guide/ADA/git_databricks.html) under the section 'Setting up a connetion to Azure DevOps'.

Once you have succesfully linked your Databricks account to DevOps you can then clone this repository using the following steps:

1. Click the 'Workspace' option in the left hand side bar
2. Within your 'Home' directory click the blue 'Create' button and select 'Git folder'
3. In the 'Create Git folder' pop-up window 
   1. enter the URL for this repo into the 'Git repository URL' box, then change the 'Git provider' to 'Azure DevOps Services'
   2. The 'Git folder name' will auto-populate with the name of the repo, you can leave this as-is or change it if you wish
   3. Click the blue 'Create Git Folder' button
   4. You can now close DevOps and open the repository in Databricks to work through the exercises in order
  
## Begin the exercises

Start the exercises from folder `01 - SQL Editor`, beginning with the README in that folder, then following the files in numerical order.

Be sure to read and follow the instructions carefully as you are running through the code to ensure you understand what is happening.

From folder `02 - Notebooks` all the exercises are stored in notebooks in sequential order.

# Contribute
If you want to add lessons to this repository you should create a new branch by clicking the git button in the top left corner of this notebook. It will always have the title of the branch you are on, so presuming you just cloned this repo it will currently say 'main'.

![Git button](images/git-button.png)

This will allow you to create a new branch where you can add a new folder for the exercise you want to contribute. Once you have completed writing and testing it, you can raise a pull request on the DevOps repository [`databricks_code_learn`](https://dfe-gov-uk.visualstudio.com/official-statistics-production/_git/databricks_code_learn).