# Databricks notebook source
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Input Widgets for Repo URL
# Input Widgets for the Repo URL, Project Name, and Workspace URL
dbutils.widgets.text(name = "repo_url", defaultValue="")
dbutils.widgets.text(name = "project", defaultValue="")
dbutils.widgets.text(name = "workspace_url", defaultValue="")
dbutils.widgets.text(name = "branch", defaultValue="main")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for PAT")

# COMMAND ----------

repo_url = dbutils.widgets.get(name="repo_url")
project = dbutils.widgets.get(name="project")
workspace_url = dbutils.widgets.get(name="workspace_url")
branch = dbutils.widgets.get(name="branch")
print(
f"""
  repo_url = {repo_url}
  project = {project}
  workspace_url = {workspace_url}
  branch = {branch}
"""
)

# COMMAND ----------

user_name = spark.sql("select current_user()").collect()[0][0]
secret_scope = user_name.split(sep="@")[0].replace(".", "-")
secret_scope

# COMMAND ----------

db_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("pat_secret")
)

db_pat

# COMMAND ----------

import dabAssist
import subprocess
from tempfile import TemporaryDirectory

# COMMAND ----------

dc = dabAssist.databricksCli(
  workspace_url = workspace_url
  ,db_pat = db_pat
)
dc

# COMMAND ----------

dc.install()

# COMMAND ----------

dc.configure().returncode

# COMMAND ----------

import json
print(
  json.dumps(
    json.loads(dc.validate().stdout.decode('utf-8'))
    ,indent=4
  )
)

# COMMAND ----------

Dir = TemporaryDirectory()
temp_directory = Dir.name

# COMMAND ----------

temp_directory

# COMMAND ----------

bundle = dabAssist.assetBundle(
  directory = temp_directory
  ,repo_url = repo_url
  ,project = project
  ,cli_path = dc.cli_path
  ,target = "dev"
)

# COMMAND ----------

bundle

# COMMAND ----------

print(
  bundle.clone()
)

# COMMAND ----------

print(
  bundle.checkout(
    branch = branch
  )
)

# COMMAND ----------

print(
  bundle.validate()
)

# COMMAND ----------

print(
  bundle.deploy()
)

# COMMAND ----------

print(
  bundle.run(
    key = "unity_catalog_setup_job"
  )
)

# COMMAND ----------

# cmd = f"{dc.cli_path} bundle run -h"
# !{cmd}

# COMMAND ----------

# print(
#   bundle.run(
#     key = "dlt_dropbox_bronze"
#     ,pipeline_flag = "--validate-only"
#   )
# )

# COMMAND ----------

print(
  bundle.destroy()
)

# COMMAND ----------

print(
  bundle.remove_clone()
)

# COMMAND ----------

# MAGIC %environment
# MAGIC "client": "1"
# MAGIC "base_environment": ""
