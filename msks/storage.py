
import os
import logging
import json
import re
import shutil
from time import time, sleep
from threading import Condition
from datetime import datetime
from typing import Optional, Callable, List

import filelock

from msks import dict_hash, logger
from msks.config import get_config
from msks.environment import Environment
from msks.task import Task, TaskWatcher, TaskStatus, _META_DIRECTORY

_REFERENCE_PARSER = re.compile(r"@(:?[a-zA-Z0-9_]+):(:?[^ ]+)")

class TaskStore(object):

    def filepath(self, file: str):
        pass

    def append_log(self, lines):
        pass

    def log(self):
        pass

    def set(self, key, value):
        pass

    def get(self, key):
        pass

    def filepath(self, file: str):
        pass

    def read(self, file, binary=False):
        pass

    def write(self, file, binary=False):
        pass

    def lock(self):
        pass

    def unlock(self):
        pass

    def clear(self):
        pass

    def __enter__(self):
        self.lock()

    def __exit__(self, exc_type, exc_value, traceback):
        self.unlock()

class TasksStore(object):

    def update(self, identifier = None):
        pass

    def wait(self, tasks=None, timeout=-1):
        pass

    def tag(self, task, tag):
        pass

    def tags(self, task):
        pass

    def remove(self, task):
        pass
        
    def cleanup(self, failed=True, completed=False):
        pass

    def query(self, filter: Optional[Callable] = None, order: Optional[Callable] = None, reverse: bool = False) -> List[Task]:
        pass

    def dependencies(self, task, wait=False):
        pass
    
    def get(self, identifier) -> Task:
        pass

    def search(self, prefix) -> Task:
        pass
    
    def restore(self):
        pass
    
    def export(self, source, entrypoint, arguments):
        pass

    def create(self, source, entrypoint, arguments, exist_ok=True) -> Task:
        pass

class FileTaskStore(TaskStore):

    def __init__(self, root):
        self._root = root
        os.makedirs(os.path.join(self._root, _META_DIRECTORY), exist_ok=True)
        self._lock = filelock.FileLock(os.path.join(self._root, _META_DIRECTORY, ".lock"))
        self._meta = None
        self._timestamp = None
        self._log = None

    def filepath(self, file: str):
        if os.path.isabs(file):
            raise IOError("Only relative paths allowed")

        return os.path.join(self._root, file)

    def append_log(self, line):
        if self._log is None:
            self._log = open(os.path.join(self._root, _META_DIRECTORY, "output.txt"), "w")

        if line is not None:
            self._log.write(line)
            self._log.flush()
        else:
            self._log.close()
            self._log = None

    def log(self):
        logfile = os.path.join(self._root, _META_DIRECTORY, "output.txt")
        if os.path.exists(logfile):
            return open(logfile).read()
        return ""

    def set(self, key: str, value):
        if key.startswith("#"):
            key = key[1:]
            self._update()
            if key in self._meta:
                if isinstance(value, (float, int, str)) and self._meta[key] == value:
                    return
            self._meta[key] = value
            self._push()
        else:
            if isinstance(value, (bytes, bytearray)):
                datafile = os.path.join(self._root, _META_DIRECTORY, "%s.blob" % key)
                with open(datafile, "wb") as handle:
                    handle.write(value)
            else:
                datafile = os.path.join(self._root, _META_DIRECTORY, "%s.json" % key)
                with open(datafile, "w") as handle:
                    json.dump(value, handle)

    def get(self, key):
        if key.startswith("#"):
            key = key[1:]
            self._update()
            return self._meta.get(key, None)
        else:
            datafile = os.path.join(self._root, _META_DIRECTORY, "%s.blob" % key)
            if os.path.isfile(datafile):
                with open(datafile, "rb") as handle:
                    return handle.read()
            datafile = os.path.join(self._root, _META_DIRECTORY, "%s.json" % key)
            if os.path.isfile(datafile):
                with open(datafile, "r") as handle:
                    return json.load(handle)
            return None

    def _update(self):
        with self._lock:
            metafile = os.path.join(self._root, _META_DIRECTORY, "meta.json")
            if self._timestamp != os.stat(metafile).st_ctime or self._meta is None:
                with open(metafile, "r") as handle:
                    self._meta = json.load(handle)
                self._timestamp = os.stat(metafile).st_ctime

    def filepath(self, file: str):
        return os.path.join(self._root, file)

    def clear(self):
        with self._lock:
            for entry in os.scandir(self._root):
                if entry.name == _META_DIRECTORY:
                    continue
                if entry.is_dir:
                    shutil.rmtree(entry.path, ignore_errors=True)
                else:
                    os.unlink(entry.path)

    def read(self, file, binary=False):
        full = self.filepath(file)
        if binary:
            return open(full, mode="rb")
        else:
            return open(full, mode="r", newline="") 

    def write(self, file, binary=False):
        full = self.filepath(file)
        if binary:
            return open(full, mode="wb")
        else:
            return open(full, mode="w", newline="") 

    def _push(self):
        with self._lock:
            metafile = os.path.join(self._root, _META_DIRECTORY, "meta.json")
            self._meta["updated"] = str(datetime.utcnow())
            with open(metafile, "w") as handle:
                json.dump(self._meta, handle)
            self._timestamp = os.stat(metafile).st_ctime

    def lock(self):
        self._lock.acquire()

    def unlock(self):
        self._lock.release()

class LocalTasksStore(TasksStore):

    def __init__(self, root):
        self._root = root
        self._condition = Condition()
        self._filelock = filelock.FileLock(os.path.join(self._root, ".lock"))
        os.makedirs(self._root, exist_ok=True)
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

        root = os.path.join(self._root, identifier)
        task = Task(FileTaskStore(root), identifier)

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

    def get(self, identifier) -> Task:
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


_store = None

def get_tasks_store() -> TasksStore:
    global _store
    if _store is None:
        scheme = get_config().store.scheme
        if scheme == "file":
            _store = LocalTasksStore(get_config().store.path)
        else:
            raise RuntimeError("Unrecognized store type")
    return _store

class TaskQueue(object):

    def __init__(self, storage: TasksStore):
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