# Databricks notebook source
# DBTITLE 1,Databricks Input Widgets for Project Setup
# Input Widgets for the Project Name, and Workspace URL 
# dbutils.widgets.text(name = "repo_url", defaultValue="")
dbutils.widgets.text(name = "project", defaultValue="")
dbutils.widgets.text(name = "workspace_url", defaultValue="")

# Add a widget for the Databricks Secret representing the Databricks Personal Access Token  
dbutils.widgets.text("pat_secret", "databricks_pat", "DB Secret for Databricks PAT")
dbutils.widgets.text("gh_pat_secret", "gh_pat", "DB Secret for Github PAT")

# optional: comma separated list of existing job ids to generate yaml for
dbutils.widgets.text(name = "existing_job_ids", defaultValue="")

# optional: comma separated list of existing pipeline ids to generate yaml for
dbutils.widgets.text(name = "existing_pipeline_ids", defaultValue="")

# COMMAND ----------

# DBTITLE 1,Retrieve inputs from Dataricks Widgets
# repo_url = dbutils.widgets.get(name="repo_url")
project = dbutils.widgets.get(name="project")
workspace_url = dbutils.widgets.get(name="workspace_url")
print(
f"""
  project = {project}
  workspace_url = {workspace_url}
"""
)

# COMMAND ----------

# DBTITLE 1,Generating Secret Scope from Current User in Spark
user_name = spark.sql("select current_user()").collect()[0][0]
secret_scope = user_name.split(sep="@")[0].replace(".", "-")
secret_scope

# COMMAND ----------

# DBTITLE 1,Get Personal Access Token from Secrets
db_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("pat_secret")
)

db_pat

# COMMAND ----------

# DBTITLE 1,Get PAT for GitHub from Secrets
gh_pat = dbutils.secrets.get(
  scope = secret_scope
  ,key = dbutils.widgets.get("gh_pat_secret")
)

gh_pat

# COMMAND ----------

# DBTITLE 1,Create the DAB inititization config file
import json

# Create dab_init_config json 
# note that this is the default-python specificiation based on https://github.com/databricks/cli/blob/a13d77f8eb29a6c7587509721217a137039f20d6/libs/template/templates/default-python/databricks_template_schema.json#L3
dab_init_config = {
    "project_name": project
    ,"include_notebook": "yes"
    ,"include_dlt": "no"
    ,"include_python": "no"
}
dab_init_config = json.dumps(dab_init_config)

# Print dab_init_config as formatted JSON
print(json.dumps(json.loads(dab_init_config), indent=4))

# COMMAND ----------

# DBTITLE 1,Display the Home Directory for the User
# MAGIC %sh
# MAGIC
# MAGIC cd ~; pwd;

# COMMAND ----------

# DBTITLE 1,Creating  a temp direcotry in the User's Home DIrectory
from pathlib import Path
from tempfile import TemporaryDirectory

home_dir = str(Path.home())
Dir = TemporaryDirectory(dir=home_dir)
temp_directory = Dir.name

temp_directory

# COMMAND ----------

# DBTITLE 1,Writing Configuration to JSON in Python
with open(f"{temp_directory}/dab_init_config.json", "w") as file:
    file.write(dab_init_config)

# COMMAND ----------

# DBTITLE 1,Use shell to cat the file
import subprocess

result = subprocess.run(f"cat {temp_directory}/dab_init_config.json", shell=True, capture_output=True)
result.stdout.decode("utf-8")

# COMMAND ----------

# DBTITLE 1,Import the dabAssist classes
import dabAssist

# COMMAND ----------

# DBTITLE 1,Create the databricks CLI object
dc = dabAssist.databricksCli(
  workspace_url = workspace_url
  ,db_pat = db_pat
)
dc

# COMMAND ----------

# DBTITLE 1,Install the CLI
dc.install()

# COMMAND ----------

# DBTITLE 1,Configure the CLI Using PAT
dc.configure().returncode

# COMMAND ----------

# DBTITLE 1,Development:  Reload dabAssist
from importlib import reload
reload(dabAssist)

# COMMAND ----------

# DBTITLE 1,Create a Databricks Asset Bundle object
bundle = dabAssist.assetBundle(
  directory = temp_directory
  ,repo_url = ""  # note that repo URL is not used when its not known yet
  ,project = project
  ,cli_path = dc.cli_path
  ,target = "dev"
)

# COMMAND ----------

# DBTITLE 1,Initialize a Databricks Asset Bundle Project with a template and config file
print(
  bundle.initialize(
    template = "default-python"
    ,config_file = "dab_init_config.json"
  )
)

# COMMAND ----------

# DBTITLE 1,Verify the Asset Bundle's Creation
cmd = f"ls -altR {temp_directory}"

result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Generate YAMLs for Existing Jobs
existing_job_ids = dbutils.widgets.get("existing_job_ids").split(",")

if len(existing_job_ids) > 0 and existing_job_ids != ['']:
  for i in existing_job_ids:
    print(
      bundle.generate_yaml(
        existing_id = i.strip()
        ,type = "job"
      )
    )

# COMMAND ----------

# DBTITLE 1,Generate YAMLs for Existing Pipelines
existing_pipeline_ids = dbutils.widgets.get("existing_pipeline_ids").split(",")

if len(existing_pipeline_ids) > 0 and existing_pipeline_ids != ['']:
  for i in existing_pipeline_ids:
    print(
      bundle.generate_yaml(
        existing_id = i.strip()
        ,type = "pipeline"
      )
    )

# COMMAND ----------

cmd = f"ls -altR {temp_directory}"

result = subprocess.run(cmd, shell=True, capture_output=True)
print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Demo Only
# cmd = f"cd {temp_directory}/{project}/resources/; pwd; cat giglia_1081964970114387_synthea_data_generation.yml"

# result = subprocess.run(cmd, shell=True, capture_output=True)
# print(result.stdout.decode("utf-8"))

# COMMAND ----------

# DBTITLE 1,Install the GitHub CLI
print(
  bundle.gh_install()
)

# COMMAND ----------

# DBTITLE 1,Write GH PAT to Temp File
with open(f"{temp_directory}/gh.txt", "w") as file:
    file.write(gh_pat)

# COMMAND ----------

# DBTITLE 1,Authenticate GitHub CLI
bundle.gh_auth(
  github_token = gh_pat
)

# COMMAND ----------

# DBTITLE 1,Create Remote Repo and Push DAB Contents
print(
  bundle.gh_repo(
    user_email = "matthew.giglia@databricks.com"
    ,user_name = "M Giglia"
    ,private = False
  )
)
