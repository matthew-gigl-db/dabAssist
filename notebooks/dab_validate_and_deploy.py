# Databricks notebook source
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Set Up Notebook Parameters
# Input Widgets for the Repo URL, Project Name, and Workspace URL
dbutils.widgets.text(name = "repo_url", defaultValue="")
dbutils.widgets.text(name = "project", defaultValue="")
dbutils.widgets.text(name = "workspace_url", defaultValue="")
dbutils.widgets.text(name = "branch", defaultValue="main")
dbutils.widgets.text(name = "job_key", defaultValue="")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for PAT")

# COMMAND ----------

# DBTITLE 1,Capture Inputs
repo_url = dbutils.widgets.get(name="repo_url")
project = dbutils.widgets.get(name="project")
workspace_url = dbutils.widgets.get(name="workspace_url")
branch = dbutils.widgets.get(name="branch")
job_key = dbutils.widgets.get(name="job_key")
print(
f"""
  repo_url = {repo_url}
  project = {project}
  workspace_url = {workspace_url}
  branch = {branch}
  job_key = {job_key}
"""
)

# COMMAND ----------

# DBTITLE 1,Determine Secret Scope
user_name = spark.sql("select current_user()").collect()[0][0]
secret_scope = user_name.split(sep="@")[0].replace(".", "-")
secret_scope

# COMMAND ----------

# DBTITLE 1,Retrieve Databricks PAT Secret
db_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("pat_secret")
)

db_pat

# COMMAND ----------

# DBTITLE 1,Import dabAssist and other Python Modules
import sys
from pathlib import Path

# Add the parent directory to sys.path
parent_dir = str(Path("../dabAssist/dabAssist.py").resolve().parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from dabAssist import dabAssist
import subprocess
from tempfile import TemporaryDirectory

# COMMAND ----------

# DBTITLE 1,Initializing Databricks CLI with dabAssist Module
dc = dabAssist.databricksCli(
  workspace_url = workspace_url
  ,db_pat = db_pat
)
dc

# COMMAND ----------

# DBTITLE 1,Install the Databricks CLI
dc.install()

# COMMAND ----------

# DBTITLE 1,Configure the Databricks CLI
dc.configure().returncode

# COMMAND ----------

# DBTITLE 1,Validate that the Databricks CLI is Configured
import json
print(
  json.dumps(
    json.loads(dc.validate().stdout.decode('utf-8'))
    ,indent=4
  )
)

# COMMAND ----------

# DBTITLE 1,Create a temporary Directory in the User's Home
from pathlib import Path

home_dir = str(Path.home())
Dir = TemporaryDirectory(dir = home_dir)
temp_directory = Dir.name

temp_directory

# COMMAND ----------

# DBTITLE 1,Creating an Asset Bundle with DabAssist
bundle = dabAssist.assetBundle(
  directory = temp_directory
  ,repo_url = repo_url
  ,project = project
  ,cli_path = dc.cli_path
  ,target = "dev"
)

# COMMAND ----------

# DBTITLE 1,Display the Bundle
bundle

# COMMAND ----------

# DBTITLE 1,Clone the Remote Repo to the Local Directory on the Cluster
print(
  bundle.clone()
)

# COMMAND ----------

# DBTITLE 1,Checkout the Feature Branch
print(
  bundle.checkout(
    branch = branch
  )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ***
# MAGIC
# MAGIC ### While Developing, iterate over: 
# MAGIC - Validate
# MAGIC - Deploy
# MAGIC - Run 

# COMMAND ----------

# DBTITLE 1,Validate the Bundle's YAMLs and Set Up
print(
  bundle.validate()
)

# COMMAND ----------

# DBTITLE 1,Deploy the Bundle to the Target
print(
  bundle.deploy()
)

# COMMAND ----------

# DBTITLE 1,Run a Job Or Pipeline Based on the Key
print(
  bundle.run(
    key = job_key
  )
)

# COMMAND ----------

# DBTITLE 1,Validate Only Pipeline Run
# print(
#   bundle.run(
#     key = "dlt_dropbox_bronze"
#     ,pipeline_flag = "--validate-only"
#   )
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ***

# COMMAND ----------

# DBTITLE 1,Destroy the Asset Bundle (Removes from Target)
print(
  bundle.destroy()
)

# COMMAND ----------

# DBTITLE 1,Remove the Clone from the Home DIrectory on the Cluster
print(
  bundle.remove_clone()
)
