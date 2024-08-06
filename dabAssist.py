# functions to assist in using Databricks Asset Bundles directly on Databricks

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

class assetBundle:

    def __init__(
      self
      ,directory: str
      ,repo_url: str
      ,project: str
      ,cli_path: str
      ,target: str = "dev"
    ):
      self.directory = directory
      self.repo_url = repo_url
      self.project = project
      self.target = target
      self.bundle_path = f"{self.directory}/{self.project}"
      self.cli_path = cli_path

    def __repr__(self):
        return f"""assetBundle(directory='{self.directory}', repo_url='{self.repo_url}', project='{self.project}', target='{self.target}', bundle_path='{self.bundle_path}', cli_path='{self.cli_path}')"""
      
    def initialize(self, template: str = "default-python", config_file: str = "dab_init_config.json"):
      cmd = f"cd {self.directory}; pwd; {self.cli_path} bundle init {template} --config-file {self.directory}/{config_file}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
    
    def gh_install(self):
      cmd = f"curl -sS https://webi.sh/gh | sh;"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
    
    def gh_auth(self, github_token: str, gh_path: str = "~/.local/bin/gh"):
      cmd = f"{gh_path} auth login --with-token < {self.directory}/gh.txt;"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
    
    #  git add .; git commit -m 'initial commit';
    def gh_repo(self, user_email: str, user_name: str, gh_path: str = "~/.local/bin/gh"):
      cmd = f"cd {self.directory}/{self.project}; pwd; git init; git config user.email '{user_email}'; git config user.name '{user_name}'; git add *; git commit -m 'initial commit'; git branch -M main; {gh_path} repo create {self.project} --private --source={self.directory}/{self.project} --remote=upstream --push;"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def clone(self):
      cmd = f"cd {self.directory}; pwd; git clone {self.repo_url}; cd {self.project}; ls -alt;"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def remove_clone(self):
      cmd = f"rm -rf {self.bundle_path}/.git; rm -rf {self.directory}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")  
    
    def checkout(self, branch: str):
      cmd = f"cd {self.bundle_path}; git checkout {branch}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def validate(self):
      cmd = f"""cd {self.bundle_path}; pwd; git pull; {self.cli_path} bundle validate -t {self.target}"""
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def deploy(self, force: bool = False):
      if force:
        cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle deploy -t {self.target} --force"
      else:
        cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle deploy -t {self.target}"
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")

    def destroy(self):
      cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle destroy -t {self.target} --auto-approve"    
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
    
    def run(self, key: str, pipeline_flag: str = "--validate-only"):
      cmd = f"cd {self.bundle_path}; pwd; {self.cli_path} bundle run -t {self.target} {pipeline_flag} {key}"    
      result = subprocess.run(cmd, shell=True, capture_output=True)
      return result.stdout.decode("utf-8") + "\n" + result.stderr.decode("utf-8")
  
  

