

import os
import json
import shutil
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Callable, List

import filelock
from msks.log import Multiplexer, PrintOutput, LogHandler
from watchgod import AllWatcher

from msks import logger
from msks.environment import Environment, Entrypoint

_META_DIRECTORY = ".meta"

class StoreOutput(LogHandler):

    def __init__(self, store):
        self._store = store

    def __call__(self, line):
        self._store.append_log(line)

    def contents(self):
        return self._store.log()

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

    def __init__(self, store: "TaskStore", identifier: str):
        self._store = store
        self._identifier = identifier
        self._env = None
        self._entrypoint = None

    def _rundir(self):
        from msks.config import get_config
        return os.path.join(get_config().runtime, self.identifier)

    def run(self, force=False, dependencies=[], output=True):

        rundir = self._rundir()

        with self._store:
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
                    dest_file = os.path.join(rundir, did + "_" + file)
                    if not os.path.exists(source_file):
                        self._status(TaskStatus.FAILED)
                        raise RuntimeError("File not found in dependency {}: {}".format(did, source_file))
                    if os.path.exists(dest_file):
                        os.unlink(dest_file)
                    logger.debug("Linking dependency %s to %s", source_file, dest_file)
                    # TODO: add readonly flag?
                    os.symlink(source_file, dest_file)

            self._status(TaskStatus.PREPARING)

        env = Environment(self.source)
        if not env.setup():
            self._status(TaskStatus.FAILED)
            return False

        command = self._store.get("#command")

        logger.info("Running task: %s", " ".join(command))
        self._status(TaskStatus.RUNNING)

        class DataWriter(object):
            def __init__(self, store: "TaskStore", key: str) -> None:
                self._store = store
                self._key = key

            def __call__(self, content):
                self._store.set(self._key, content)

        os.makedirs(rundir, exist_ok=True)

        runlock = filelock.FileLock(os.path.join(rundir, ".runlock"))

        with runlock:
            logs = [StoreOutput(self._store)]

            if output:
                logs.append(PrintOutput())

            for i, observer in enumerate(self.entrypoint.observers):
                writer = DataWriter(self._store, "observer_%d" % i)
                logs.append(observer.handler(writer))

            if env.run(*command, cwd=rundir,
                env=self.entrypoint.environment, 
                output=Multiplexer(*logs)):

                artifacts = []
                for entry in os.scandir(rundir):
                    if entry.is_file:
                        for pattern in self.entrypoint.artifacts:
                            filename = os.path.basename(entry.path)
                            if pattern(filename):
                                artifacts.append(filename)
                                continue
                        
                for artifact in artifacts:
                    logger.debug("Saving artifact %s", artifact)

                    with open(os.path.join(rundir, artifact), "rb") as source:
                        with self._store.write(artifact, binary=True) as dest:
                            shutil.copyfileobj(source, dest)

                self._status(TaskStatus.COMPLETE)

                shutil.rmtree(rundir, ignore_errors=True)

                return True
            else:
                self._status(TaskStatus.FAILED)
                return False

    def _status(self, status: TaskStatus):
        assert isinstance(status, TaskStatus)
        self._store.set("#status", str(status))

    def reset(self, clear=False):
        with self._store:
            if clear:
                self._store.clear()
            self._status(TaskStatus.PENDING)

    @property
    def identifier(self):
        return self._identifier

    @property
    def dependencies(self):
        deps = self._store.get("#dependencies")
        return deps if deps is not None else {}

    @property
    def status(self):
        status = self._store.get("#status")
        return TaskStatus(status if status is not None else "unknown")

    @property
    def created(self) -> datetime:
        datestr = self._store.get("#created")
        if datestr is None:
            return None
        return datetime.strptime(datestr, '%Y-%m-%d %H:%M:%S.%f').astimezone()

    @property
    def updated(self) -> datetime:
        datestr = self._store.get("#updated")
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
        return self._store.get("#entrypoint")

    @property
    def entrypoint(self) -> Entrypoint:
        with self._store:
            if self._entrypoint is not None:
                return self._entrypoint
            cache = self._store.get("entrypoint")
            if cache is not None :
                self._entrypoint = Entrypoint(**cache)
                return self._entrypoint
            else:
                self._entrypoint = self.environment.entrypoints[self.entrypoint_name]
                self._store.set("entrypoint", self._entrypoint.dump())
                return self._entrypoint

    @property
    def arguments(self):
        args = self._store.get("#arguments")
        return self.entrypoint.coerce(args) if args is not None else {}

    def argument(self, name):
        args = self.entrypoint.merge(self.arguments, True)
        return args.get(name, None)

    @property
    def log(self):
        return self._store.log()

    @property
    def source(self):
        repository = self._store.get("#repository")
        commit = self._store.get("#commit")
        return repository + "#" + commit

    @property
    def repository(self):
        return self._store.get("#repository")

    @property
    def commit(self):
        return self._store.get("#commit")

    @property
    def tags(self):
        # TODO: how to do this?
        return [] #self._storage.tags(self)

    def filepath(self, file: str):
        return self._store.filepath(file)

    def get(self, key: str, default: Optional[Any] = None):
        properties = self._store.get("#properties")
        if properties is None:
            return default
        if not key in properties:
            return default
        return properties[key]

    @property
    def properties(self):
        properties = self._store.get("#properties")
        if properties is None:
            return dict()
        return dict(**properties)

    def set(self, key: str, value: Any):
        with self._lock:
            properties = self._store.get("#properties")
            if properties is None:
                properties = {}
            if key in properties:
                if properties[key] == value:
                    return False
            self._store.set("#properties", properties)
            return True

_filter_claims = {
    "failed": lambda x: x.status == TaskStatus.FAILED,
    "pending": lambda x: x.status == TaskStatus.PENDING,
    "complete": lambda x: x.status == TaskStatus.COMPLETE,
    "running": lambda x: x.status == TaskStatus.RUNNING,
    "entrypoint": lambda x: x.entrypoint_name,
    "source": lambda x: x.source,
    "commit": lambda x: x.commit,
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
        from .filters import Expression
        self._expression = Expression.parseString(condition)

    def __call__(self, task: Task):

        return self._expression(TaskFilter.TaskSymbolAdapter(task))

