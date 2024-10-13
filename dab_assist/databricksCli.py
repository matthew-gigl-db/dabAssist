# class and methods for installing and configuring the databricks cli for the current user on serverless compute (using the Databricks PAT and a workspace host url)

import subprocess
from tempfile import TemporaryDirectory

class databricksCli:

    def __init__(
      self
      ,workspace_url: str
      ,db_pat: str
    ):
      self.workspace_url = workspace_url
      self.db_pat = db_pat

    def __repr__(self):
      return f"""databricksCli(workspace_url='{self.workspace_url}', db_pat='{self.db_pat}')"""
    
    def install(self) -> str:
      cmd = f"curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      response = result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
      if response[0:4] == "Inst":
        cli_path = response.split("at ")[1].replace(".", "").strip("\n")
      else:
        cli_path = response.split(" ")[2].strip("\n")
      version = subprocess.run(f"{cli_path} --version", shell=True, capture_output=True)
      print(f"databricks-cli installed at {cli_path} version {version.stdout.decode('utf-8').strip()}")
      self.cli_path = cli_path
      return cli_path
    
    def configure(self):
      cmd= f"""echo '{self.db_pat}' | {self.cli_path} configure --host '{self.workspace_url}'"""
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result
    
    def validate(self):
      cmd = f"{self.cli_path} current-user me"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result
      
#######################  
#######################
#######################