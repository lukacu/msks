
from builtins import Exception
import shutil
import sys
import os
import argparse
from re import search

#from typing_extensions import runtime_checkable
import argcomplete
import logging
import time
from threading import Thread

from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table
from rich.tree import Tree

from msks import logger
from msks.task import TaskFilter, TaskStatus
from msks.storage import TaskQueue, get_tasks_store
from msks.environment import Entrypoint, Environment

console = Console()

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via raw_input() and return their answer.

    "question" is a string that is presented to the user.
    "default" is the presumed answer if the user just hits <Enter>.
        It must be "yes" (the default), "no" or None (meaning
        an answer is required of the user).

    The "answer" return value is True for "yes" or False for "no".
    """
    valid = {"yes": True, "y": True, "ye": True,
             "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError("invalid default answer: '%s'" % default)

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == '':
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            sys.stdout.write("Please respond with 'yes' or 'no' "
                             "(or 'y' or 'n').\n")

def process_arguments(raw):
    state = {}
    if raw is not None:
        for var in raw:
            key, value = var.split("=")
            state[key] = value
    return state

def resolve_source(source):
    from msks.config import get_config

    config = get_config()

    return config.aliases.get(source, source)


def task_submit(source, entrypoint, arguments, properties=None, tags=None, force=False):

    tasks = get_tasks_store()

    r = tasks.create(resolve_source(source), entrypoint, arguments, exist_ok=force)

    if r is None:
        logger.error("Task already exists")
        return

    if tags is not None:
        for tag in tags:
            tasks.tag(r, tag)

    if properties is not None:
        for key, value in properties.items():
            r.set(key, value)

def task_export(identifier, filename):

    tasks = get_tasks_store()
    task = tasks.get(identifier)

    if task is None:
        raise RuntimeError("Task does not exist")

    filepath = task.filepath(filename)

    if os.path.isfile(filepath):
        shutil.copy(filepath, os.curdir)
        logger.info("Copied file %s from task %s", filename, task.identifier)
    elif os.path.isdir(filepath):
        count = [0]
        def copy_count(src, dst):
            shutil.copy2(src, dst)
            count[0] += 1
        shutil.copytree(filepath, os.path.join(os.curdir, os.path.basename(filepath)), copy_function=copy_count)
        logger.info("Copied %d files %s from task %s", count[0], filename, task.identifier)
    else:
        raise RuntimeError("File does not exist")

def task_script(source, entrypoint, arguments):

    tasks = get_tasks_store()

    command = tasks.export(resolve_source(source), entrypoint, arguments)

    print(command)

def do_shell(source):

    env = Environment(resolve_source(source))
    env.setup()
    env.shell()  

def task_execute(source, entrypoint, arguments, force=False, properties=None, tags=None, remove=False):

    tasks = get_tasks_store()

    r = tasks.create(resolve_source(source), entrypoint, arguments)

    if tags is not None:
        for tag in tags:
            tasks.tag(r, tag)
    if properties is not None:
        for key, value in properties.items():
            r.set(key, value)

    dependencies = tasks.dependencies(r, wait=True)

    if dependencies is None:
        raise RuntimeError("Unable to execute task due to problem with dependencies")

    if r.run(force, dependencies=dependencies):
        logger.info("Task %s complete", r.identifier)
    else:
        logger.info("Task %s failed", r.identifier)
        if remove:
            tasks.remove(r)

def do_queue(workers):

    from multiprocessing.pool import ThreadPool

    tasks = get_tasks_store()
    queue = TaskQueue(tasks)

    def worker():
        while True:
            task, dependencies = queue.get()
            if task is None:
                break

            logger.info("Executing task %s", task.identifier)

            try:

                if task.run(dependencies=dependencies, output=False):
                    logger.info("Task %s complete", task.identifier)
                else:
                    logger.info("Task %s failed", task.identifier)

            except Exception as e:
                logger.fatal(e)

    logger.debug("Starting %d workers" % workers)

    workers = [Thread(target=worker) for i in range(workers)]

    for worker in workers:
        worker.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down workers")
        pass

    queue.close()
    
    for worker in workers:
        worker.join()

def _tasks_table(tasks, store: "TasksStore"):

    table = Table(title="Tasks", expand=True)

    table.add_column("ID", justify="left", style="cyan", no_wrap=True)
    table.add_column("Status", style="magenta")
    table.add_column("Tags", justify="right")
    table.add_column("Created", justify="right")
    table.add_column("Updated", justify="right")

    for task in tasks:
        table.add_row(task.identifier, task.status.color(), ", ".join(store.tags(task)), str(task.created), str(task.updated))

    return table

def list_tasks(query):

    tasks = get_tasks_store()

    if query:
        logger.debug("Using query: %s", " ".join(query))
        query = TaskFilter(" ".join(query))
    else:
        query = None

    table = _tasks_table(tasks.query(query), tasks)

    console.print(table)

def task_tag(identifier, tag):

    tasks = get_tasks_store()
    task = tasks.get(identifier)

    if task is None:
        raise RuntimeError("Task does not exist")

    tasks.tag(task, tag)

def task_info(identifier, recursive=False):

    tasks = get_tasks_store()
    task = tasks.get(identifier)

    if task is None:
        raise RuntimeError("Task does not exist")

    def info(task):
        tree = Tree(task.identifier)
        tree.add("[bold]Status[/bold]: {}".format(task.status.color()))
        tree.add("[bold]Created[/bold]: {}".format(str(task.created)))
        tree.add("[bold]Updated[/bold]: {}".format(str(task.updated)))
        tree.add("[bold]Tags[/bold]: {}".format(", ".join(tasks.tags(task))))

        if task.properties:
            prop = tree.add("[bold]Properties[/bold]")
            for k, v in task.properties.items():
                tasks.add("{} = {}".format(k, v))

        tree.add("[bold]Source[/bold]: {}".format(task.source))
        tree.add("[bold]Entrypoint[/bold]: {}".format(task.entrypoint_name))

        args = tree.add("[bold]Arguments[/bold]")

        entrypoint = task.entrypoint

        for k, v in entrypoint.arguments.items():
            if k in task.arguments:
                args.add("[bold]{} = {}[/bold]".format(k, task.arguments[k]))
            else:
                args.add("[grey30]{} = {}[/]".format(k, v.default))

        if task.dependencies:
            deps = tree.add("[bold]Dependencies[/bold]")
            for dtask in task.dependencies:
                if recursive:
                    deps.add(info(tasks.get(dtask)))
                else:
                    deps.add(dtask)
        return tree

    console.print(info(task))

def task_entrypoints(source):

    source = resolve_source(source)

    env = Environment(source)

    def describe(name, entrypoint: Entrypoint):
        tree = Tree(name)

        tree.add("[bold]Command[/bold]: {}".format(entrypoint.command))
        args = tree.add("[bold]Arguments[/bold]")

        for k, v in entrypoint.arguments.items():
            if v.default is None:
                args.add("[red]{}[/red]".format(k))
            else:
                args.add("{} = {}".format(k, v.default))
                
        envs = tree.add("[bold]Environment variables[/bold]")

        for k, v in entrypoint.environment.items():
            envs.add("{} = {}".format(k, v))

        logs = tree.add("[bold]Observers[/bold]")
        if entrypoint.observers.iterations is not None:
            logs.add("Iteration measures observer")

        if entrypoint.observers.aggregate is not None:
            logs.add("Measure aggregator")

        return tree

    tree = Tree(source)

    for name, entrypoint in env.entrypoints.items():
        tree.add(describe(name, entrypoint))

    console.print(tree)

def task_log(identifier):

    tasks = get_tasks_store()
    task = tasks.get(identifier)

    if task is None:
        logger.error("Task does not exist")
        return

    console.print(task.log)

def task_remove(query, force=False, yes=False):

    tasks = get_tasks_store()

    query = TaskFilter(" ".join(query))

    selected = tasks.query(query)

    if not selected:
        logger.info("No tasks matched given criteria")
        return

    if not yes:
        console.print(_tasks_table(selected, tasks))
        if not query_yes_no("Remove these tasks?", "no"):
            return

    for task in selected:
        if task.status in (TaskStatus.PREPARING, TaskStatus.RUNNING) and not force:
            logger.error("Cannot remove an active task, use --force to override")
            return

        tasks.remove(task)
        logger.info("Removed task %s", task.identifier)

def do_cleanup(yes=False):

    tasks = get_tasks_store()

    environments = Environment.list_environments()
    sources = Environment.list_sources()

    for task in tasks.query():
        env = task.environment
        try:
            environments.remove(env.environment_identifier)
        except:
            pass
        try:
            sources.remove(env.source_identifier)
        except:
            pass

    if len(environments) == 0 and len(sources) == 0:
        logger.info("Nothing to remove")
        return

    if not yes:
        if not query_yes_no("Remove %d environments and %d sources?" % (len(environments), len(sources)), "no"):
            return

    Environment.remove_environments(*environments)
    Environment.remove_sources(*sources)

def task_reset(query, force=False, remove=False, yes=False):

    tasks = get_tasks_store()

    query = TaskFilter(" ".join(query))

    selected = tasks.query(query)

    if not selected:
        logger.info("No tasks matched given criteria")
        return

    if not yes:
        console.print(_tasks_table(selected, tasks))
        if not query_yes_no("Reset these tasks?", "no"):
            return


    for task in selected:

        if task.status in (TaskStatus.PREPARING, TaskStatus.RUNNING) and not force:
            logger.error("Cannot reset an active task, use --force to override")
            return

        task.reset(remove)
        logger.info("Reset task %s", task.identifier)


def main():

    parser = argparse.ArgumentParser(description='Manages multiple experiment tasks and organizes results', prog="msks")
    parser.add_argument("--debug", "-d", default=False, help="Turn on debug", required=False, action='store_true')
    subparsers = parser.add_subparsers(help='commands', dest='action', title="Commands")

    queue_parser = subparsers.add_parser('queue', help='Start a processing queue that executes available tasks')
    queue_parser.add_argument("--workers", default=1, required=False, help='Number of tasks executed in parallel', type=int)

    notify_parser = subparsers.add_parser('notify', help='Watches results storage and notifies on status change')

    run_parser = subparsers.add_parser('run', help='Execute a task')
    run_parser.add_argument("--tag", "-t", action='append', help='Tag task with human readable name', type=str)
    run_parser.add_argument("--property", "-p", dest="properties", action='append', help='Set property for the task', type=str)
    run_parser.add_argument("--force", "-f", default=False, help="Force task even if already completed", required=False, action='store_true')
    run_parser.add_argument("--rm", default=False, help="Remove task if it failed", required=False, action='store_true')
    run_parser.add_argument("source", help='Source repository to clone')
    run_parser.add_argument('entrypoint', help='Entrypoint that you want to execute')
    run_parser.add_argument('arguments', nargs=argparse.REMAINDER, help='Arguments for the entrypoint')

    script_parser = subparsers.add_parser('script', help='Export task script for external execution')
    script_parser.add_argument("source", help='Source repository to clone')
    script_parser.add_argument('entrypoint', help='Entrypoint that you want to execute')
    script_parser.add_argument('arguments', nargs=argparse.REMAINDER, help='Arguments for the entrypoint')

    shell_parser = subparsers.add_parser('shell', help='Creates environment and opens an interactive shell')
    shell_parser.add_argument("source", help='Source repository to clone')

    export_parser = subparsers.add_parser('export', help='Copy file from task directory to current directory')
    export_parser.add_argument('identifier', help='Task identifier')
    export_parser.add_argument('filename', help='Name of a file in task directory')
    
    list_parser = subparsers.add_parser('list', help='List tasks')
    list_parser.add_argument('query', nargs=argparse.REMAINDER, help='Filter query', type=str)

    tag_parser = subparsers.add_parser('tag', help='Add tag to task')
    tag_parser.add_argument('identifier', help='Task identifier')
    tag_parser.add_argument('tag', help='Tag name')

    info_parser = subparsers.add_parser('info', help='Get info about a task')
    info_parser.add_argument("--recursive", "-r", default=False, help="Recurse into dependencies", required=False, action='store_true')
    info_parser.add_argument('identifier', help='Task identifier')

    log_parser = subparsers.add_parser('log', help='Get log output from a task')
    log_parser.add_argument('identifier', help='Task identifier')

    remove_parser = subparsers.add_parser('remove', help='Remove a task')
    remove_parser.add_argument("--force", "-f", default=False, help="Force removal even if already completed", required=False, action='store_true')
    remove_parser.add_argument("--yes", "-y", default=False, help="Do not require confirmation", required=False, action='store_true')
    remove_parser.add_argument('query', nargs=argparse.REMAINDER, help='Filter query', type=str)

    reset_parser = subparsers.add_parser('reset', help='Reset a task')
    reset_parser.add_argument("--force", "-f", default=False, help="Force reset even if already completed", required=False, action='store_true')
    reset_parser.add_argument("--rm", default=False, help="Remove data when resetting state", required=False, action='store_true')
    reset_parser.add_argument("--yes", "-y", default=False, help="Do not require confirmation", required=False, action='store_true')
    reset_parser.add_argument('query', nargs=argparse.REMAINDER, help='Filter query', type=str)

    submit_parser = subparsers.add_parser('submit', help='Submit a task, do not execute it')
    submit_parser.add_argument("--tag", "-t", action='append', help='Tag task with human readable name', type=str)
    submit_parser.add_argument("--force", "-f", default=False, help="Force task even if already exists", required=False, action='store_true')
    submit_parser.add_argument("--property", "-p", dest="properties", action='append', help='Set property for the task', type=str)
    submit_parser.add_argument("source", help='Source repository to clone')
    submit_parser.add_argument('entrypoint', help='Entrypoint that you want to execute')
    submit_parser.add_argument('arguments', nargs=argparse.REMAINDER, help='Arguments for the entrypoint')

    entrypoints_parser = subparsers.add_parser('entrypoints', help='Describes entrypoints for given source')
    entrypoints_parser.add_argument("source", help='Source repository to use')

    validate_parser = subparsers.add_parser('validate', help='Validates entrypoints file')
    validate_parser.add_argument("source", help='Entrypoints file to validate')

    cleanup_parser = subparsers.add_parser('cleanup', help='Removes environments and sources that are no longer attached to any task')
    cleanup_parser.add_argument("--yes", "-y", default=False, help="Do not require confirmation", required=False, action='store_true')

    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    logger.setLevel(logging.INFO)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.addHandler(RichHandler(rich_tracebacks=args.debug, console=Console(stderr=True)))
    else:
        logger.addHandler(logging.StreamHandler())

    try:

        if args.action == "run":
            task_execute(args.source, args.entrypoint, process_arguments(args.arguments), properties=process_arguments(args.properties), force=args.force, tags=args.tag, remove=args.rm)
        elif args.action == "list":
            list_tasks(args.query)
        elif args.action == "tag":
            task_tag(args.identifier, args.tag)
        elif args.action == "info":
            task_info(args.identifier, recursive=args.recursive)
        elif args.action == "log":
            task_log(args.identifier)
        elif args.action == "remove":
            task_remove(args.query, force=args.force, yes=args.yes)
        elif args.action == "reset":
            task_reset(args.query, force=args.force, remove=args.rm, yes=args.yes)
        elif args.action == "submit":
            task_submit(args.source, args.entrypoint, 
                process_arguments(args.arguments), 
                properties=process_arguments(args.properties), tags=args.tag, force=args.force)
        elif args.action == "script":
            task_script(args.source, args.entrypoint, process_arguments(args.arguments))
        elif args.action == "shell":
            do_shell(args.source)
        elif args.action == "export":
            task_export(args.identifier, args.filename)
        elif args.action == "entrypoints":
            task_entrypoints(args.source)
        elif args.action == "validate":
            from msks.environment import Entrypoints
            try:
                config = Entrypoints.read(args.source)
                config.dump()
                print("Valid entrypoint file")
            except AttributeError as ae:
                print("Invalid syntax: {}".format(ae))

        elif args.action == "cleanup":
            do_cleanup(yes=args.yes)
        elif args.action == "queue":
            do_queue(args.workers)


        elif args.action == "notify":
            from msks.config import get_config

            channels = get_config().notify
            if len(channels) == 0:
                raise RuntimeError("No notification channels configured")
            from .notify import watch
            watch(channels)

        else:
            parser.print_help()

    except Exception as e:
        logger.exception(e)
        sys.exit(-1)