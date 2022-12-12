from argparse import ArgumentParser
from concurrent.futures import as_completed, ThreadPoolExecutor
from json import dumps
import logging
from os import getenv, path, walk
from re import match
import uuid
from subprocess import CalledProcessError, PIPE, Popen, STDOUT


class JsonFormatter(logging.Formatter):

  def __init__(self, fmt_dict: dict = None, time_format: str = "%Y-%m-%dT%H:%M:%S", msec_format: str = "%s.%03dZ"):
    self.fmt_dict = fmt_dict if fmt_dict is not None else {"message": "message"}
    self.default_time_format = time_format
    self.default_msec_format = msec_format
    self.datefmt = None

  def usesTime(self) -> bool:
    return "asctime" in self.fmt_dict.values()

  def formatMessage(self, record) -> dict:
    result_dict = {}
    for fmt_key, fmt_val in self.fmt_dict.items():
      if fmt_key == "message" and type(record.__dict__["msg"]) is dict:
        result_dict[fmt_key] = record.__dict__["msg"]
        continue
      result_dict[fmt_key] = record.__dict__[fmt_val]
    return result_dict

  def format(self, record) -> str:
    record.message = record.getMessage()

    if self.usesTime():
        record.asctime = self.formatTime(record, self.datefmt)

    message_dict = self.formatMessage(record)

    if record.exc_info:
      if not record.exc_text:
        record.exc_text = self.formatException(record.exc_info)

    if record.exc_text:
      message_dict["exc_info"] = record.exc_text

    if record.stack_info:
      message_dict["stack_info"] = self.formatStack(record.stack_info)

    return dumps(message_dict, default=str, indent=2)


logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
stream_handler=logging.StreamHandler()
stream_handler.setFormatter(JsonFormatter({"level": "levelname", 
                                           "message": "message", 
                                           "loggerName": "name", 
                                           "processName": "processName",
                                           "processID": "process", 
                                           "threadName": "threadName", 
                                           "threadID": "thread",
                                           "timestamp": "asctime"}))
logger.addHandler(stream_handler)


class Diff:
  def __init__(self, path: str, output: str, status: int, error: str = None, lock_id: str = None) -> None:
    self.path = path
    self.output = output
    self.error = error
    self.exit_status = status
    self.lock_id = lock_id


class AWSTerragrunt:

  def __init__(self, AWS_ACCESS_KEY: str, AWS_SECRET_KEY: str, AWS_SESSION_TOKEN :str) -> None:
    self.__auth_envs  = "AWS_ACCESS_KEY_ID={} AWS_SECRET_ACCESS_KEY={} AWS_SESSION_TOKEN={}".format(
      AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN
    )

  @staticmethod
  def validate_terragrunt() -> None:
    cmd = "terragrunt --version"
    try:
      p = Popen(cmd, shell=True, universal_newlines=True,
                stdout=PIPE, stderr=STDOUT)
      output, err = p.communicate()
    except CalledProcessError as e:
      logging.critical("CalledProcessError exception occurred", exc_info=True)

  def __run_proccess(self, cmd: str, path: str, func_uuid: str = None):
    logger.debug({"msg": "Running run_proccess function", "uuid": func_uuid})
    try:
      p  = Popen(cmd, shell=True,  universal_newlines=True,
                 stdout=PIPE, stderr=STDOUT, cwd=path)
      output, error = p.communicate()
      return output, error, p.returncode
    except CalledProcessError as e:
      logging.critical("CalledProcessError exception occurred", exc_info=True)

  def __get_lock_id(self, raw_output: str) -> str:
    for line in raw_output.split(('\n')):
      if match("^ *ID: *([0-9,a-z]*-){4}[0-9,a-z]*$", line):
        return line[line.rfind(' ')+1:]
    return None

  def get_plan(self, path: str) -> Diff:
    func_uuid = str(uuid.uuid4())
    logger.debug({"msg": "Running get_plan function", "uuid": func_uuid})
    cmd = f"{self.__auth_envs} terragrunt plan -no-color -detailed-exitcode"
    output, error, returncode = self.__run_proccess(cmd, path, func_uuid)
    if returncode == 1:
      return Diff(path, output, returncode, error, self.__get_lock_id(output))
    return Diff(path, output, returncode, error)

  def force_unlcok(self, path: str, lock_id: str) -> Diff:
    func_uuid = str(uuid.uuid4())
    logger.debug({"msg": "Running force_unlcok function", "uuid": func_uuid})
    cmd = f"{self.__auth_envs} terragrunt force-unlock -force {lock_id}"
    _, _, _ = self.__run_proccess(cmd, path, func_uuid)
    return self.get_plan(path)


def get_dirs(root_dir: str, exclude: list = [".terragrunt-cache"]) -> list:
  result_list = []
  for root, dirs, files in walk(path.abspath(root_dir)):
    dirs[:] = [d for d in dirs if d not in exclude]
    if "terragrunt.hcl" in files:
      result_list.append(root)
  return result_list

def format_message(message: list, msg_start: str = None, msg_end: str = None) -> list:
  first_line_flag = True
  line_number = 0
  index_start, index_end = 0, len(message)
  for line in message:
    if match(msg_start, line) and first_line_flag:
      index_start = line_number
      first_line_flag = False
    if u'\u2500' in line:
      message[line_number] = ""
    if match(msg_end, line):
      index_end = line_number
    line_number+=1
  return message[index_start+1:index_end-1]

def main():

  logger.debug("Debug enabled")

  parser = ArgumentParser(description='Terragrunt observability tool')
  parser.add_argument("-r", "--root",     help="the root directory of script",  default='.')
  parser.add_argument("-w", "--workers",  help="a count of parallel thread",    default=30 )
  args = parser.parse_args()

  AWSTerragrunt.validate_terragrunt()
  tg = AWSTerragrunt(getenv("AWS_ACCESS_KEY_ID"), getenv("AWS_SECRET_ACCESS_KEY"), getenv("AWS_SESSION_TOKEN"))

  diffs = []

  with ThreadPoolExecutor(max_workers=args.workers) as executor:
    locked_result=[]
    plans = [executor.submit(tg.get_plan, path) for path in get_dirs(args.root)]
    for p in as_completed(plans):
      result = p.result()
      if result.lock_id is not None:
        locked_result.append(result)
        continue
      if result.exit_status != 0:
        result.output = format_message(result.output.split('\n'), "^$", "^You can apply this plan.*$")
        diffs.append(result)
    plans = [executor.submit(tg.force_unlcok, r.path, r.lock_id) for r in locked_result]
    for p in as_completed(plans):
      result = p.result()
      if result.exit_status != 0:
        result.output = format_message(result.output.split('\n'), "^$", "^You can apply this plan.*$")
        diffs.append(result)


  count = 0
  for i in sorted(diffs, key=lambda p: p.path, reverse=True):
    logger.info({"path": i.path, "diff": i.output})
    count += 1
  logger.info(f"You need to fix {count} states")


if __name__ == '__main__':
  main()
