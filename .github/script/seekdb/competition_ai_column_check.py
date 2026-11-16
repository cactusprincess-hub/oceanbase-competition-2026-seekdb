#!/usr/bin/env python3
"""Run existing AI column mysqltests with the production runner on Linux."""
import importlib.util
import sys
from argparse import Namespace
from pathlib import Path

root = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("mysqltest_runner", Path(__file__).with_name("mysqltest_for_seekdb.py"))
runner = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = runner
spec.loader.exec_module(runner)
label = sys.argv[1]
cases = [case for case in runner.discover_cases(root) if case.name.startswith("ai_column.")]
regression = root / "unittest/sql/ai_column"
if regression.is_dir():
    cases.append(runner.MysqltestCase("ai_column.ai_column_scheduling",
        regression / "t/ai_column_scheduling.test",
        regression / "r/mysql/ai_column_scheduling.result"))
if len(sys.argv) > 2 and sys.argv[2] == "critical":
    cases = [case for case in cases if case.name in (
        "ai_column.ai_column_worker", "ai_column.ai_column_retry_strict",
        "ai_column.ai_column_scheduling")]
    order = {"ai_column.ai_column_worker": 0,
             "ai_column.ai_column_retry_strict": 1,
             "ai_column.ai_column_scheduling": 2}
    cases.sort(key=lambda case: order[case.name])
if len(sys.argv) > 2 and sys.argv[2] == "cascade":
    by_name = {case.name: case for case in cases}
    cases = [by_name["ai_column.ai_column_worker"],
             by_name["ai_column.ai_column_retry_strict"]]
    original_prepare = runner.prepare_instance
    prepared = False

    def prepare_once(*arguments):
        global prepared
        if not prepared:
            original_prepare(*arguments)
            prepared = True

    runner.prepare_instance = prepare_once
runner.discover_cases = lambda _: cases
runner.MAX_CASE_RETRIES = 0
original_sdb = runner.run_sdb


def run_sdb(script, command, arguments, description, cwd):
    if command == "start":
        arguments = tuple(arguments) + ("--nodaemon", "--parameter", "datafile_size=256M",
            "--parameter", "datafile_maxsize=1G", "--parameter", "log_disk_size=8G",
            "--parameter", "cpu_count=4")
    return original_sdb(script, command, arguments, description, cwd)


runner.run_sdb = run_sdb
args = Namespace(seekdb=root / "build_release/src/observer/seekdb",
    obclient=root / "deps/3rd/u01/obclient/bin/obclient",
    mysqltest=root / "deps/3rd/u01/obclient/bin/mysqltest",
    base_dir=Path("/tmp/competition-ai-column-instance"),
    work_dir=root / ("linux-ai-column-" + label), host="127.0.0.1", port=2991,
    slice_index=0, slice_count=1)
raise SystemExit(runner.command_run(args))
