

import os
import logging
import json
import re
import shutil
from time import time, sleep
from threading import Condition
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Callable, List

import filelock
from msks.log import FileOutput, FileWriter, IterativeMeasuresAggregator, MeasuresAggregator, Multiplexer, PrintOutput
from watchgod import AllWatcher

from msks import dict_hash, logger
from msks.environment import Environment, Entrypoint

_META_DIRECTORY = ".meta"

_REFERENCE_PARSER = re.compile(r"@(:?[a-zA-Z0-9_]+):(:?[^ ]+)")

class TaskWatcher(AllWatcher):

    def should_watch_dir(self, entry: os.DirEntry) -> bool:
        # Watch for changes in root dir (adding new tasks, tags)
        if entry.name == _META_DIRECTORY:
            return True
        if entry.path == self.root_path:
            return True
        if os.path.dirname(entry.path) == self.root_path:
            return True

        return False

    def should_watch_file(self, entry: os.DirEntry) -> bool:
        # Only watch metadata files for changes in task status
        return entry.path.endswith(os.path.join(_META_DIRECTORY, "meta.json"))

class TaskStatus(Enum):

    UNKNOWN = "unknown"
    PENDING = "pending"
    PREPARING = "preparing"    
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    ARCHIVED = "archived"

    def color(self):
        if self == TaskStatus.PENDING:
            style = "blue"
        elif self == TaskStatus.RUNNING:
            style = "yellow"
        elif self == TaskStatus.PREPARING:
            style = "orange"
        elif self == TaskStatus.COMPLETE:
            style = "green bold"
        elif self == TaskStatus.FAILED:
            style = "red bold"
        elif self == TaskStatus.ARCHIVED:
            style = "gray"
        elif self == TaskStatus.UNKNOWN:
            style = "gray"
        return "[{}]{}[/]".format(style, self)

    def __str__(self):
        return self.value

class Task(object):

    def __init__(self, storage: "TaskStorage", identifier):
        self._storage = storage
        self._root = os.path.join(storage.root, identifier)
        self._identifier = identifier
        self._lock = filelock.FileLock(os.path.join(self._root, _META_DIRECTORY, ".lock"))
        self._runlock = filelock.FileLock(os.path.join(self._root, _META_DIRECTORY, ".runlock"))
        self._meta = None
        self._env = None
        self._entrypoint = None

    def run(self, force=False, dependencies=[], output=True):

        with self._lock:
            self.update()

            if self.status == TaskStatus.COMPLETE and not force:
                logger.info("Task already done, aborting.")
                return True

            if self.status == TaskStatus.FAILED and not force:
                logger.info("Task already done, aborting.")
                return False

            if self.status == TaskStatus.RUNNING:
                logger.info("Task already in progress, aborting.")
                return True

            dependencies = {d.identifier: d for d in dependencies}
            for did, files in self.dependencies.items():
                if not did in dependencies:
                    self._status(TaskStatus.FAILED)
                    raise RuntimeError("Dependency not found")
                if dependencies[did].status != TaskStatus.COMPLETE:
                    self._status(TaskStatus.FAILED)
                    raise RuntimeError("Dependent task not complete: {}".format(did))
                for file in files:
                    source_file = dependencies[did].filepath(file)
                    dest_file = os.path.join(self._root, did + "_" + file)
                    if not os.path.exists(source_file):
                        self._status(TaskStatus.FAILED)
                        raise RuntimeError("File not found in dependency {}: {}".format(did, source_file))
                    if os.path.exists(dest_file):
                        os.unlink(dest_file)
                    logger.debug("Linking dependency %s to %s", source_file, dest_file)
                    os.symlink(source_file, dest_file)

            self._status(TaskStatus.PREPARING)

        env = Environment(self._meta["repository"] + "@" + self._meta["commit"])
        if not env.setup():
            self._status(TaskStatus.FAILED)
            return False

        command = self._meta["command"]

        logger.info("Running task: %s", " ".join(command))
        self._status(TaskStatus.RUNNING)

        class DataWriter(FileWriter):

            def __init__(self, task: Task, filename) -> None:
                self._task = task
                self._filename = filename

            def __call__(self, content):
                with self._task._lock:
                    with open(self._filename, "w") as handle:
                        handle.write(content)

        with self._runlock:
            logs = [FileOutput(os.path.join(self._root, _META_DIRECTORY, "output.txt"))]

            if output:
                logs.append(PrintOutput())

            if self.entrypoint.observers.iterations is not None:
                writer = DataWriter(self, os.path.join(self._root, _META_DIRECTORY, "data_iterative.json"))
                logs.append(IterativeMeasuresAggregator(writer=writer, **self.entrypoint.observers.iterations.dump()))

            if self.entrypoint.observers.aggregate is not None:
                writer = DataWriter(self, os.path.join(self._root, _META_DIRECTORY, "data_aggregated.json"))
                logs.append(MeasuresAggregator(writer=writer, **self.entrypoint.observers.aggregate.dump()))

            if env.run(*command, cwd=self._root,
                env=self.entrypoint.environment, 
                output=Multiplexer(*logs)):

                self._status(TaskStatus.COMPLETE)
                return True
            else:
                self._status(TaskStatus.FAILED)
                return False

    def update(self):
        with self._lock:
            metafile = os.path.join(self._root, _META_DIRECTORY, "meta.json")
            with open(metafile, "r") as handle:
                self._meta = json.load(handle)

    def restore(self):
        with self._lock:
            if self._meta is None:
                self.update()
            if self.status == TaskStatus.RUNNING:
                if not os.path.exists(self._runlock.lock_file):
                    self._status(TaskStatus.FAILED)
                else:
                    try:
                        self._runlock.acquire(0.1)
                        self._update(TaskStatus.PENDING)
                        self._runlock.release()
                    except filelock.Timeout:
                        pass

    def _status(self, status: TaskStatus):
        assert isinstance(status, TaskStatus)
        self._meta["status"] = str(status)
        self._meta["updated"] = str(datetime.utcnow())
        self._push()

    def _push(self):
        with self._lock:
            metafile = os.path.join(self._root, _META_DIRECTORY, "meta.json")
            self._timestamp = os.stat(metafile).st_ctime
            with open(metafile, "w") as handle:
                json.dump(self._meta, handle)

    def reset(self, clear=False):
        with self._lock:
            if clear:
                for entry in os.scandir(self._root):
                    if entry.name == _META_DIRECTORY:
                        continue
                    if entry.is_dir:
                        shutil.rmtree(entry.path, ignore_errors=True)
                    else:
                        os.unlink(entry.path)

            self._status(TaskStatus.PENDING)

    @property
    def identifier(self):
        return self._identifier

    @property
    def dependencies(self):
        if self._meta is None:
            self.update()
        return self._meta.get("dependencies", {})

    @property
    def status(self):
        if self._meta is None:
            self.update()
        return TaskStatus(self._meta.get("status", "unknown"))

    @property
    def created(self) -> datetime:
        if self._meta is None:
            self.update()
        datestr = self._meta.get("created", None)
        if datestr is None:
            return None
        return datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S.%f').astimezone()

    @property
    def updated(self) -> datetime:
        if self._meta is None:
            self.update()
        datestr = self._meta.get("updated", None) 
        if datestr is None:
            return None
            
        return datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S.%f').astimezone()

    @property
    def environment(self) -> Environment:
        if self._env is None:
            self._env = Environment(self.source)
        return self._env

    @property
    def entrypoint_name(self) -> str:
        if self._meta is None:
            self.update()
        return self._meta.get("entrypoint", None) 

    @property
    def entrypoint(self) -> Entrypoint:
        with self._lock:
            if self._entrypoint is not None:
                return self._entrypoint
            cachefile = os.path.join(self._root, _META_DIRECTORY, "entrypoint.yaml")
            if os.path.isfile(cachefile):
                self._entrypoint = Entrypoint.read(cachefile)
                return self._entrypoint
            else:
                self._entrypoint = self.environment.entrypoints[self.entrypoint_name]
                self._entrypoint.write(cachefile)
                return self._entrypoint

    @property
    def arguments(self):
        if self._meta is None:
            self.update()
        return self._meta.get("arguments", {}) 

    def argument(self, name):
        if self._meta is None:
            self.update()
        args = self._meta.get("arguments", {}) 
        args = self.entrypoint.merge(args, True)
        return args.get(name, None)


    def data(self, name):
        if self._meta is None:
            self.update()
        datafile = os.path.join(self._root, _META_DIRECTORY, "data_%s.json" % name)
        if os.path.isfile(datafile):
            with open(datafile, "r") as handle:
                return json.load(handle)
        return None


    @property
    def log(self):
        if self._meta is None:
            self.update()
        logfile = os.path.join(self._root, _META_DIRECTORY, "output.txt")
        if os.path.exists(logfile):
            return open(logfile).read()
        return ""

    @property
    def source(self):
        if self._meta is None:
            self.update()
        return self._meta["repository"] + "@" + self._meta["commit"]

    @property
    def tags(self):
        return self._storage.tags(self)

    def filepath(self, file: str):
        if os.path.isabs(file):
            raise IOError("Only relative paths allowed")

        return os.path.join(self._root, file)

    def read(self, file, binary=False):
        full = self.filepath(file)

        if binary:
            return open(full, mode="rb")
        else:
            return open(full, mode="r", newline="")

    def get(self, key: str, default: Optional[Any] = None):
        if self._meta is None:
            self.update()
        if not "properties" in self._meta:
            return default
        if not key in self._meta["properties"]:
            return default
        return self._meta["properties"][key]

    @property
    def properties(self):
        if self._meta is None:
            self.update()
        if not "properties" in self._meta:
            return dict()
        return dict(**self._meta["properties"])

    def set(self, key: str, value: Any):
        with self._lock:
            self.update()
            
            if not "properties" in self._meta:
                self._meta["properties"] = {}
            if key in self._meta["properties"]:
                if self._meta["properties"][key] == value:
                    return False
            self._meta["properties"][key] = value
            self._meta["updated"] = str(datetime.now())
            self._push()

_filter_claims = {
    "failed": lambda x: x.status == TaskStatus.FAILED,
    "pending": lambda x: x.status == TaskStatus.PENDING,
    "complete": lambda x: x.status == TaskStatus.COMPLETE,
    "running": lambda x: x.status == TaskStatus.RUNNING,
    "entrypoint": lambda x: x.entrypoint_name,
    "created": lambda x: x.created,
    "updated": lambda x: x.updated
}

class TaskFilter(Callable):

    class TaskSymbolAdapter(object):

        def __init__(self, task: Task):
            self._task = task

        def __getitem__(self, name: str):
            if name.startswith("&"):
                return self._task.get(name[1:])
            if name.startswith("@"):
                arguments = self._task.arguments
                return arguments.get(name[1:], None)
            if name.startswith("#"):
                tags = self._task.tags
                return name[1:] in tags

            if name in _filter_claims:
                return _filter_claims[name](self._task)

            return self._task.identifier == name

        def __contains__(self, name: str):
            return self[name] is not None

    def __init__(self, condition):
        from filtration import Expression
        self._expression = Expression.parseString(condition)

    def __call__(self, task: Task):

        return self._expression(TaskFilter.TaskSymbolAdapter(task))


class TaskStorage(object):

    def __init__(self):
        root = os.environ.get("MSKS_STORAGE", os.path.join(os.environ["HOME"], ".local", "msks"))

        self._root = os.environ.get("MSKS_TASKS", os.path.join(root, "tasks"))
        self._condition = Condition()
        self._filelock = filelock.FileLock(os.path.join(root, ".lock"))
        os.makedirs(root, exist_ok=True)
        self._tasks = dict()
        self._tags = dict()
        self._watcher = None
        self.update()

    def update(self, identifier = None):

        with self._condition:
            if identifier is None:
                with self._filelock:
                    self._tasks = dict()
                    self._tags = dict()

                    tasks = [f.path for f in os.scandir(self._root) if f.is_dir()]
                    tags = [f.path for f in os.scandir(self._root) if f.is_file()]

                    for task in tasks:
                        if os.path.isfile(os.path.join(task, _META_DIRECTORY, "meta.json")):
                            self._load(os.path.basename(task))

                    for tag in tags:
                        taskid = open(tag, "r").read().strip()
                        tagname = os.path.basename(tag)
                        if taskid in self._tasks:
                            self._tags[tagname] = taskid

                    self._watcher = TaskWatcher(self._root)

            elif identifier in self._tasks:
                self._load(os.path.join(self._root, identifier))

    def wait(self, tasks=None, timeout=-1):
        if tasks is not None:
            tasks = [task.identifier if isinstance(task, Task) else task for task in tasks]
            if any([task not in self._tasks for task in tasks]):
                raise RuntimeError("Unknown task")

        start = time()

        while True:
            changes = self._watcher.check()
            elapsed = max(0, time() - start)
            if changes:
                return True
            else:
                if timeout > 0 and elapsed >= timeout:
                    return False
                else:
                    sleep(0.1)

    def _load(self, identifier):

        task = Task(self, identifier)

        self._tasks[identifier] = task
        return self._tasks[identifier]

    def tag(self, task, tag):
        if isinstance(task, Task):
            task = task.identifier

        with self._condition:
            with self._filelock:
                self._tags[tag] = task
                open(os.path.join(self._root, tag), "w").write(task)
                logger.debug("Tagging task %s as %s", task, tag)

    def tags(self, task):
        if isinstance(task, Task):
            task = task.identifier

        with self._condition:
            tags = [t for t, v in self._tags.items() if v == task]
            return tags

    def remove(self, task):
        if isinstance(task, Task):
            task = task.identifier

        with self._condition:
            with self._filelock:
                if not task in self._tasks:
                    return
                for tag in self.tags(task):
                    logger.debug("Removing tag %s", tag)
                    os.unlink(os.path.join(self._root, tag))

                logger.debug("Removing task %s", task)
                shutil.rmtree(os.path.join(self._root, task), ignore_errors=True)
                del self._tasks[task]
                
        
    def cleanup(self, failed=True, completed=False):
        with self._condition:
            with self._filelock:
                remove = []
                for identifier, data in self._tasks.items():
                    if (data.status == "failed" and failed) or (data.status == "completed" and completed):
                        remove.append(identifier)
                for identifier in remove:
                    self.remove(identifier)

    def get(self, identifier, search=False) -> Task:
        with self._condition:

            if identifier in self._tags:
                identifier = self._tags[identifier]

            if not identifier in self._tasks:
                candidates = self.search(identifier)
                if len(candidates) == 1:
                    return self._tasks[candidates[0]]

                return None
            return self._tasks[identifier]

    def search(self, prefix) -> Task:
        with self._condition:
            return [k for k in self._tasks if k.startswith(prefix)]
    
    def restore(self):
        with self._condition:
            with self._filelock:
                for _, task in self._tasks.items():
                    task.restore()
    
    def export(self, source, entrypoint, arguments):
        env = Environment(source)

        if entrypoint not in env.entrypoints:
            raise RuntimeError("Entrypoint not found")

        with self._condition:
            arguments, _ = self._normalize_arguments(arguments, global_paths=True)
            command = env.entrypoints[entrypoint].generate(arguments)

            return env.export(*command, env=env.entrypoints[entrypoint].environment)

    def create(self, source, entrypoint, arguments, exist_ok=True) -> Task:
        env = Environment(source)

        if entrypoint not in env.entrypoints:
            raise RuntimeError("Entrypoint not found")

        with self._condition:
            with self._filelock:

                arguments, dependencies = self._normalize_arguments(arguments)
                command = env.entrypoints[entrypoint].generate(arguments)

                meta = {"repository": env.repository, "commit" : env.commit,
                     "entrypoint" : entrypoint, "arguments": arguments, "dependencies": dependencies}

                taskid = dict_hash(dict(repository=env.repository, commit=env.commit, entrypoint=entrypoint,
                    arguments=env.entrypoints[entrypoint].merge(arguments), dependencies=dependencies))

                if taskid in self._tasks:
                    return self.get(taskid) if exist_ok else None

                meta["status"] = str(TaskStatus.PENDING)
                meta["command"] = command
                meta["created"] = str(datetime.utcnow())
                meta["updated"] = str(datetime.utcnow())
                meta["environment"] = dict(env.entrypoints[entrypoint].environment.items())

                task_storage = os.path.join(self._root, taskid)

                os.makedirs(os.path.join(task_storage, _META_DIRECTORY), exist_ok=True)

                task_meta = os.path.join(task_storage, _META_DIRECTORY, "meta.json")

                with open(task_meta, "w") as handle:
                    json.dump(meta, handle)

                if taskid in self._tasks:
                    return self.get(taskid)

                return self._load(taskid)

    def query(self, filter: Optional[Callable] = None, order: Optional[Callable] = None, reverse: bool = False) -> List[Task]:
        with self._condition:
            results = []
            for identifier, data in self._tasks.items():
                if filter is None or filter(data):
                    results.append(data)
            
            if not order is None:
                return sorted(results, key=order, reverse=reverse)
            else:
                return results

    def dependencies(self, task, wait=False):

        if not isinstance(task, Task):
            task = self.get(task)

        while True:

            with self._condition:
                dependencies = []
                ready = True
                for depencency in task.dependencies:
                    d = self.get(depencency)
                    if d is None or d.status == TaskStatus.FAILED:
                        return None
                    if d.status != TaskStatus.COMPLETE:
                        ready = False
                    else:
                        dependencies.append(d)

            if ready or not wait:
                break

            self.wait(task.dependencies)

        return dependencies if ready else None

    def _normalize_arguments(self, arguments, global_paths=False):

        processed = []

        dependencies = dict()

        def replace(match):
            task_ref = match.group(1)
            resource = match.group(2)
            task = self.get(task_ref)

            if task is None:
                raise RuntimeError("Dependency reference not found: {}".format(task_ref))

            argument = task.argument(resource)
            if argument is not None:
                return str(argument)

            dependencies.setdefault(task.identifier, set()).add(resource)
            if global_paths:
                return task.filepath(resource)
            else:
                return task.identifier + "_" + resource

        processed = {k : _REFERENCE_PARSER.sub(replace, v) for k, v in arguments.items()}

        dependencies = {k: sorted(list(v)) for k, v in dependencies.items()}

        return processed, dependencies

    @property
    def root(self):
        return self._root

    def __enter__(self):
        self._
        self._filelock.acquire()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._filelock.release()

class TaskQueue(object):

    def __init__(self, storage: TaskStorage):
        self._storage = storage
        self._condition = Condition()

    def get(self):

        while True:
            with self._condition:
                if self._storage is None:
                    break
                tasks = self._storage.query(filter=lambda x: x.status == TaskStatus.PENDING, order=lambda x: x.created)
                for task in tasks:
                    deps = self._storage.dependencies(task)
                    if not deps is None:
                        return task, deps

            if self._storage.wait(timeout=5):
                self._storage.update()

        return None, None

    def close(self):
        with self._condition:
            self._storage = None