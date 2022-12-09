from sys import argv
from os import walk, chdir, getenv
from os.path import abspath
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
from concurrent.futures import ThreadPoolExecutor, as_completed

class TerragruntDiff:

  def __new__(cls, **kwargs) -> None:
    #TODO added errors
    cmd = "terragrunt --version"
    try:
      p = Popen(cmd, shell=True, universal_newlines=True,
                stdout=PIPE, stderr=STDOUT)
      output, err = p.communicate()
    except CalledProcessError as err:
      print(err)
      return None
    return super().__new__(cls)

  def __DirTree(self, root_dir: str, exclude: list):
    result_file = []
    root_dir = abspath(root_dir)
    for root, dirs, files in walk(root_dir):
      dirs[:] = [d for d in dirs if d not in exclude]
      if "terragrunt.hcl" in files:
        result_file.append(root)
    return result_file

  def __GetDiffs(self, path: str, auth_envs: str):
    cmd = "terragrunt plan -no-color -detailed-exitcode -lock=false"
    diff = None
    chdir(path)
    try:
      p  = Popen(auth_envs + ' ' + cmd,
                          shell=True, universal_newlines=True,
                          stdout=PIPE, stderr=STDOUT)
      output, err = p.communicate()
#######Debug
      #print(" \n==========" +str(err) + " " + path + " ==========\n")
      #print(output)
      #print(str(p.returncode))
#######
      ### TODO add tg auto unlock
      if p.returncode in [1,2]:
        diff = output
    except CalledProcessError as err:
      return path, err
    return path, diff



class AwsTerragruntDiff(TerragruntDiff):

  def __new__(cls, **kwargs) -> None:
    #TODO added errors
    if all(key in kwargs for key in (
      "AWS_ACCESS_KEY",
      "AWS_SECRET_KEY",
      "AWS_SESSION_TOKEN"
    )):
      return super().__new__(cls)
    return None

  def __init__(self, **kwargs) -> None:
    self.__auth_envs  = "AWS_ACCESS_KEY_ID="     + str(kwargs["AWS_ACCESS_KEY"])    + " " + \
                        "AWS_SECRET_ACCESS_KEY=" + str(kwargs["AWS_SECRET_KEY"])    + " " + \
                        "AWS_SESSION_TOKEN="     + str(kwargs["AWS_SESSION_TOKEN"]) + " "

  def GetDiffs(self, path: str, exclude_path: list = [".terragrunt-cache"], max_workers: int = 15):
    result = {}
    dirs = self._TerragruntDiff__DirTree(path, exclude_path)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
      diffs = [executor.submit(self._TerragruntDiff__GetDiffs, d, self.__auth_envs) for d in dirs]
      for d in as_completed(diffs):
        diff_path, diff_value = d.result()
        if diff_value is not None:
          result[diff_path] = diff_value
    return result



if __name__ == '__main__':
    tg = AwsTerragruntDiff(AWS_ACCESS_KEY       = getenv("AWS_ACCESS_KEY_ID"), 
                           AWS_SECRET_KEY    = getenv("AWS_SECRET_ACCESS_KEY"), 
                           AWS_SESSION_TOKEN = getenv("AWS_SESSION_TOKEN"))

    if tg is None:
      print("Error of creating object")
      exit(1)

    r = tg.GetDiffs(argv[1])
    sorted_dict = dict(sorted(r.items()))
    for i in sorted_dict:
      print('===k=== ' + i )#+ "\n\n" + r[i])