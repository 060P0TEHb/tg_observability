"""
Tool for providing better terraform state observability

Input parameters:
%(root)s        The root script directory. Default is the current directory
%(workers)d     Number of parallel tasks   Default is 30

You can override this. For helping type --help
"""

import concurrent.futures
import logging
import re
import uuid

from argparse import ArgumentParser
from dataclasses import dataclass
from json import dumps
from os import getenv, path, walk
from subprocess import PIPE, STDOUT, Popen
from typing import Optional


from jinja2 import Environment, FileSystemLoader

class JsonFormatter(logging.Formatter):
    """
    Formatter that outputs JSON strings after parsing the LogRecord.

    fmt_dict        -- Key: logging format attribute pairs.
                       Defaults to {"message": "message"}.
    time_format     -- time.strftime() format string.
                       Default: "%Y-%m-%dT%H:%M:%S"
    msec_format     -- Microsecond formatting. Appended at the end
                       Default: "%s.%03dZ"
    """
    def __init__(self, fmt_dict: dict = None,
                 time_format: str = "%Y-%m-%dT%H:%M:%S",
                 msec_format: str = "%s.%03dZ"):
        #W0102: Dangerous default value {} as argument (dangerous-default-value)
        self.fmt_dict = fmt_dict if fmt_dict is not None else {"message": "message"}
        self.default_time_format = time_format
        self.default_msec_format = msec_format
        logging.Formatter.__init__(self)

    def usesTime(self) -> bool:
        """
        Overwritten to look for the attribute in the format dict values instead of the fmt string.
        """
        return "asctime" in self.fmt_dict.values()

    def formatMessage(self, record) -> dict:
        """
        Overwritten to return a dictionary of the relevant LogRecord attributes instead of a string.
        KeyError is raised if an unknown attribute is provided in the fmt_dict.
        """
        result_dict = {}
        for fmt_key, fmt_val in self.fmt_dict.items():
            if fmt_key == "message" and isinstance(record.__dict__["msg"], (list, dict)):
                result_dict[fmt_key] = record.__dict__["msg"]
                continue
            result_dict[fmt_key] = record.__dict__[fmt_val]
        return result_dict

    def format(self, record) -> str:
        """
        Mostly the same as the parent's class method, the difference being
        that a dict is manipulated and dumped as JSON instead of a string.
        """
        record.message = record.getMessage()

        if self.usesTime():
            record.asctime = self.formatTime(record, None)

        message_dict = self.formatMessage(record)

        if record.exc_info:
            if not record.exc_text:
                record.exc_text = self.formatException(record.exc_info)

        if record.exc_text:
            message_dict["exc_info"] = record.exc_text

        if record.stack_info:
            message_dict["stack_info"] = self.formatStack(record.stack_info)

        return dumps(message_dict, default=str, indent=2)


logger = logging.getLogger(__name__)
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

@dataclass
class Diff:
    """
    Class for keeping diffs of terraform states

    state_path  -- Path to terragrunt files that have changes
    output      -- The outputs from terragrunt command
    exit_status -- Terragrun process exit status
                    0 - Succeeded, diff is empty (no changes)
                    1 - Errored
                    2 - Succeeded, there is a diff
    error       -- The errors from terragrunt command,
                    default is None
    lock_id     -- The lock id of the terragrunt state,
                    default is None
    """
    state_path:     str
    output:         str
    exit_status:    int
    error:          str = None
    lock_id:        str = None


class AWSTerragrunt:

    """
    Class for applying terragrunt commands.

    aws_access_key      -- AWS_ACCESS_KEY_ID of the AWS user
    aws_secret_key      -- AWS_SECRET_ACCESS_KEY of the AWS user
    aws_session_token   -- AWS_SESSION_TOKEN of the AWS user
    """

    def __init__(self, aws_access_key: str, aws_secret_key:str,
                       aws_session_token: str) -> None:
        self.__auth_envs  = (f"AWS_ACCESS_KEY_ID={aws_access_key} "
                             f"AWS_SECRET_ACCESS_KEY={aws_secret_key} "
                             f"AWS_SESSION_TOKEN={aws_session_token}")

    @staticmethod
    def validate_terragrunt() -> None:
        """
        Checking of terragrunt to exist.
        """
        cmd = "terragrunt --version"
        with Popen(cmd, shell=True, universal_newlines=True,
                   stdout=PIPE, stderr=STDOUT) as proc_result:
            proc_result.communicate()

    def __run_proccess(self, cmd: str, state_path: str, func_uuid: str = None) -> tuple:
        """
        Running and returning output and error of process.

        Keyword arguments:
        cmd         -- running command
        state_path  -- the root directory for command running
        func_uuid   -- unique UID for a better debugging process
        """
        logger.debug({"msg": "Running run_proccess function", "uuid": func_uuid})
        with Popen(cmd, shell=True,  universal_newlines=True,
                   stdout=PIPE, stderr=STDOUT, cwd=state_path) as proc_result:
            output, error = proc_result.communicate()
            return output, error, proc_result.returncode

    def __get_lock_id(self, raw_output: str) -> Optional[str]:
        """
        Finding and returning lock_id from terraform output.

        Keyword arguments:
        raw_output  -- raw output of terragrun plan command
        """
        for line in raw_output.split(('\n')):
            if re.match("^ *ID: *([0-9,a-z]*-){4}[0-9,a-z]*$", line):
                return line[line.rfind(' ')+1:]
        return None

    def get_plan(self, state_path: str, func_uuid: str = None) -> Diff:
        """
        Running terragrunt plan and returning Diff object instance.

        Keyword arguments:
        state_path  -- the root directory for command running
        func_uuid   -- the uuid for debugging purpose
        """
        func_uuid = func_uuid if func_uuid is not None else str(uuid.uuid4())
        logger.debug({"msg": "Running get_plan function", "uuid": func_uuid})
        cmd = f"{self.__auth_envs} terragrunt plan -no-color -detailed-exitcode"
        output, error, returncode = self.__run_proccess(cmd, state_path, func_uuid)
        if returncode == 1:
            return Diff(state_path=state_path,
                        output=output,
                        exit_status=returncode,
                        error=error,
                        lock_id=self.__get_lock_id(output))
        return Diff(state_path=state_path,
                    output=output,
                    exit_status=returncode,
                    error=error)

    def force_unlock(self, state_path: str, lock_id: str, func_uuid: str = None) -> Diff:
        """
        Trying to unlock the terragrunt state, rerunning the terragrunt plan
        command, and returning the Diff object instance.

        Keyword arguments:
        state_path  -- the root directory for command running
        lock_id     -- The ID of lock state
        func_uuid   -- the uuid for debugging purpose
        """
        func_uuid = func_uuid if func_uuid is not None else str(uuid.uuid4())
        logger.debug({"msg": "Running force_unlock function", "uuid": func_uuid})
        cmd = f"{self.__auth_envs} terragrunt force-unlock -force {lock_id}"
        self.__run_proccess(cmd, state_path, func_uuid)
        return self.get_plan(state_path, func_uuid)


def get_dirs(root_dir: str, exclude_dirs: list = None) -> list:
    """
    Finding all directories, expecting excluded, to contain the terragrunt.hcl file
    and return the list of them.

    Keyword arguments:
    root_dir     -- the root directory for command running
    exclude_dirs -- the list of excluded directories,
                    default is [".terragrunt-cache"]
    """
    #W0102: Dangerous default value {} as argument (dangerous-default-value)
    exclude_dirs = exclude_dirs if exclude_dirs is not None else [".terragrunt-cache"]
    result_list = []
    for root, dirs, files in walk(path.abspath(root_dir)):
        dirs[:] = [d for d in dirs if d not in exclude_dirs]
        if "terragrunt.hcl" in files and not dirs:
            result_list.append(root)
    return result_list

def format_message(message: list, msg_start: str = None, msg_end: str = None) -> list:
    """
    Normalising and returning the output of the terragrunt plan command.

    Keyword arguments:
    message     -- the terragrunt plan output message
    msg_start   -- regular expression of beginning a new message
    msg_end     -- regular expression of ending a new message
    """
    first_line_flag = True
    line_number = 0
    index_start, index_end = 0, len(message)-3
    for line in message:
        if msg_start is not None:
            if re.match(msg_start, line) and first_line_flag:
                index_start = line_number + 1
                first_line_flag = False
        # Truncation of the symbol '???'
        if '\u2500' in line:
            message[line_number] = '\u2500'*20
        if msg_end is not None:
            if re.match(msg_end, line):
                index_end = line_number - 1
        line_number+=1
    return message[index_start:index_end]

def main():
    """ Main function of the tool """
    logger.debug("Debug enabled")

    # Initialising and configuring of argparse
    parser = ArgumentParser(description='Terragrunt observability tool')
    parser.add_argument("-r", "--root",     help="the root directory of script",  default='.')
    parser.add_argument("-w", "--workers",  help="a count of parallel thread",    default=30 )
    args = parser.parse_args()

    # Checking of the terragrunt exist
    AWSTerragrunt.validate_terragrunt()
    # Initialising of AWSTerragrunt class
    aws_tg = AWSTerragrunt(getenv("AWS_ACCESS_KEY_ID"),
                           getenv("AWS_SECRET_ACCESS_KEY"),
                           getenv("AWS_SESSION_TOKEN"))

    # Initialising of a thread pool
    diffs = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Running get_plan method for all found directories
        threads = [executor.submit(aws_tg.get_plan, path) for path in get_dirs(args.root)]
        while threads:
            for thread in concurrent.futures.as_completed(threads):
                new_threads = None
                # If the Diff object does contain not an empty lock ID field,
                # try to unlock it
                if thread.result().lock_id is not None:
                    # Running the new thread
                    new_threads = executor.submit(aws_tg.force_unlock,
                                                  thread.result().state_path,
                                                  thread.result().lock_id)
                    threads.append(new_threads)

                # Normalising the Diff.output, if it has errors or diffs
                # and appending to the result list
                if thread.result().exit_status != 0 and new_threads is None:
                    thread.result().output = format_message(thread.result().output.split('\n'))
                    diffs.append(thread.result())

                # Removing the now-completed thread
                threads.remove(thread)

    # temporary printing of the result of the tool.
    count = 0
    for i in sorted(diffs, key=lambda p: p.state_path, reverse=True):
        logger.info({"path": i.state_path, "diff": i.output})
        count += 1
    logger.info('You need to fix %s states', count)

    tool_path=path.realpath(path.dirname(__file__))
    template = Environment(loader=FileSystemLoader(tool_path)).get_template("index.j2")
    content = template.render(diffs=diffs)
    with open(f"{tool_path}/report/index.html", mode="w", encoding="utf-8") as message:
        message.write(content)

if __name__ == '__main__':
    main()
