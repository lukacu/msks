
from __future__ import absolute_import
from enum import Enum
from genericpath import exists

import os
import subprocess
import logging
import shutil
import re
import shlex
from attributee.primitives import Boolean, Primitive
from msks.log import FileOutput, Multiplexer, PrintOutput

import yaml

import git

import filelock

from attributee import String, Attributee, List, Map, Integer, Nested, Enumeration, Object
from attributee.primitives import to_logical
from attributee.io import Serializable

from msks import logger
from msks import dict_hash
from msks.config import get_config

ARGUMENT_PARSER = re.compile(r"\{\{(:?[a-zA-Z0-9_]+)\}\}")

CONDA_URL = "https://repo.anaconda.com/miniconda/Miniconda3-py37_23.1.0-1-Linux-x86_64.sh"

def _lock_conda():
    conda_dir = get_config().conda
    if not os.path.exists(conda_dir):
        os.makedirs(conda_dir, exist_ok=True)
    return filelock.FileLock(os.path.join(conda_dir, ".lock"))

def _lock_sources():
    sources_dir = get_config().sources
    if not os.path.exists(sources_dir):
        os.makedirs(sources_dir, exist_ok=True)
    return filelock.FileLock(os.path.join(sources_dir, ".lock"))

def _order_multi(item):

    def extract_key(x):
        if isinstance(x, dict):
            return list(x.keys())
        if isinstance(x, list):
            return x
        return [x]

    if isinstance(item, list):
        return sorted([_order_multi(x) for x in item], key=extract_key)
    elif isinstance(item, dict):
        item = {name: _order_multi(value) for name, value in item.items()}
        return dict(sorted(item.items()))
    return item

def _find_file(directory, options):
    for option in options:
        filepath = os.path.join(directory, option)
        if os.path.isfile(filepath):
            return filepath
    return None

def processor_resolver(typename: str, ctx, **kwargs) -> Attributee:
    from msks.log import StepsExtractor, ScoresExtractor, SequencesExtractor
    from attributee.object import default_object_resolver

    if typename == "steps":
        return StepsExtractor(**kwargs)

    if typename == "sequences" or typename == "iterative":
        return SequencesExtractor(**kwargs)

    if typename == "scores":
        return ScoresExtractor(**kwargs)

    return default_object_resolver(typename, ctx, **kwargs)

class ArgumentType(Enum):

    INT = "int"
    FLOAT = "float"
    BOOL = "bool"
    STRING = "string"

class Argument(Attributee):

    type = Enumeration(ArgumentType, default="string")
    default = Primitive(default=None)
    significant = Boolean(default=True)
    description = String(default="")

    def coerce(self, value):
        if value is None:
            return None
        if self.type == ArgumentType.INT:
            return int(value)
        elif self.type == ArgumentType.FLOAT:
            return float(value)
        elif self.type == ArgumentType.BOOL:
            return to_logical(value)
        return str(value)

class FileMatch(String):

    class Matcher(object):

        def __init__(self, pattern):
            self._pattern = pattern

        def __call__(self, filename):
            from fnmatch import fnmatch
            return fnmatch(filename, self._pattern)

    def coerce(self, value, ctx):
        value = super().coerce(value, ctx)
        return FileMatch.Matcher(value)

    def dump(self, value):
        return value._pattern


class Entrypoint(Attributee, Serializable):
    command = String()
    observers = List(Object(resolver=processor_resolver, subclass="msks.log.LogProcessor"), default=[])
    arguments = Map(Nested(Argument), default={})
    environment = Map(String(), default={})
    artifacts = List(FileMatch(), separator=";", default=[])

    def generate(self, arguments):
        arg = self.merge(arguments, True)

        def replace(match):
            if match.group(1) in arg:
                return str(arg[match.group(1)])
            else:
                raise ValueError("Argument {} not defined", match.group(1))

        return shlex.split(ARGUMENT_PARSER.sub(replace, self.command))

    def coerce(self, arguments):
        return {k: self.arguments[k].coerce(v) for k, v in arguments.items() 
                if k in self.arguments}

    def merge(self, arguments, insignificant=False):
        arg = dict([(k, v.coerce(v.default)) for k, v in self.arguments.items() if insignificant or v.significant])
        arg.update({k: self.arguments[k].coerce(v) for k, v in arguments.items() 
                if k in self.arguments and (insignificant or self.arguments[k].significant)})

        for k, v in arg.items():
            if v is None:
                raise RuntimeError("Argument {} not set.".format(k))

        return arg

class Entrypoints(Attributee, Serializable):
    version = Integer(default=1)
    entrypoints = Map(Nested(Entrypoint), default={})

class Environment(object):

    def __init__(self, source):

        if not "#" in source:
            repository = source
            commit = "master"
        else:
            repository, commit = source.split("#")

        remote_heads = git.cmd.Git().ls_remote(repository, heads=True)
        remote_heads = [head.split("\t") for head in remote_heads.split("\n")]

        for head_hash, head_name in remote_heads:
            if head_name.endswith("/" + commit):
                commit = head_hash
                break

        self._repository = repository
        self._commit = commit
        self._source_dir = None
        self._conda_env = None
        self._entrypoints = None

    @staticmethod
    def list_environments():
        with _lock_conda():
            conda_dir = get_config().conda
            return [x for x in os.listdir(conda_dir) if os.path.isdir(os.path.join(conda_dir, x, "condabin"))]

    @staticmethod
    def list_sources():
        with _lock_sources():
            sources_dir = get_config().sources
            return [x for x in os.listdir(sources_dir) if os.path.isdir(os.path.join(sources_dir, x, ".git"))]

    @staticmethod
    def remove_environments(*ids):
        with _lock_conda():
            conda_dir = get_config().conda
            for id in ids:

                if not os.path.isdir(os.path.join(conda_dir, id, "condabin")):
                    continue

                logger.debug("Removing environment %s", id)
                shutil.rmtree(os.path.join(conda_dir, id), ignore_errors=True)

    @staticmethod
    def remove_sources(*ids):
        with _lock_sources():
            sources_dir = get_config().sources
            for id in ids:

                if not os.path.isdir(os.path.join(sources_dir, id, ".git")):
                    continue

                logger.debug("Removing source %s", id)
                shutil.rmtree(os.path.join(sources_dir, id), ignore_errors=True)

    @staticmethod
    def list_sources():
        with _lock_sources():
            sources_dir = get_config().sources
            return [x for x in os.listdir(sources_dir) if os.path.isdir(os.path.join(sources_dir, x, ".git"))]


    @property
    def source_path(self):
        if self._source_dir is None:
            self._source_dir = self._setup_source(self._repository, self._commit)

        return self._source_dir

    @property
    def source_identifier(self):
        return os.path.basename(self._source_dir)

    @property
    def environment_identifier(self):

        if self._conda_env is None:

            source_dir = self.source_path

            if source_dir is None:
                return None

            conda_file = _find_file(source_dir, ["conda.yaml", "conda.yml", "env.yml"])
            pip_file = _find_file(source_dir, ["requirements.txt", "pip.txt"])
            shell_file = _find_file(source_dir, ["environment.sh", "env.sh", "install.sh", "setup.sh"])

            with open(conda_file, "r") as handle:
                config = yaml.load(handle, Loader=yaml.SafeLoader)

            if pip_file is not None:
                with open(pip_file, "r") as handle:
                    config["_pip"] = handle.readlines()

            if shell_file is not None:
                with open(shell_file, "r") as handle:
                    lines = handle.readlines()
                    lines = [line for line in lines if not line.strip().startswith("#") and not len(line.strip(" \n\r")) == 0]
                    config["_shell"] = "".join(lines)

            config.pop("name", None)
            config = _order_multi(config)
            self._conda_env = dict_hash(config)

        return self._conda_env

    def setup(self):
        source_dir = self.source_path

        if source_dir is None:
            return None

        conda_file = _find_file(source_dir, ["conda.yaml", "conda.yml", "env.yml"])
        pip_file = _find_file(source_dir, ["requirements.txt", "pip.txt"])
        shell_file = _find_file(source_dir, ["environment.sh", "env.sh", "install.sh", "setup.sh"])

        return self._setup_conda_environment(self.environment_identifier, conda_file, pip_file, shell_file)

    def _setup_conda(self):

        from msks.remote import download_file

        debug = logger.isEnabledFor(logging.DEBUG)
        output = PrintOutput() if debug else None

        with _lock_conda():
            conda_dir = get_config().conda

            if os.path.isfile(os.path.join(conda_dir, "condabin", "conda")):
                logger.debug("Conda installation found")
                return True

            logger.info("Downloading and installing Conda distributuion, this may take a while")

            installer_file = download_file(CONDA_URL)

            logger.debug("Download complete")

            success = self._run_command("sh", installer_file, "-b", '-f', '-p', conda_dir, output=output)

            if success:
                logger.debug("Installation complete")

            os.unlink(installer_file)

            return success

    def _setup_conda_environment(self, id, conda_file, pip_file=None, shell_file=None):

        debug = logger.isEnabledFor(logging.DEBUG)
        output = PrintOutput() if debug else None

        if not self._setup_conda():
            return False

        with _lock_conda():
            
            conda_dir = get_config().conda

            if os.path.isfile(os.path.join(conda_dir, id, "condabin", "conda")):
                logger.debug("Conda environment %s already exists", id)
                return True

            logger.info("Creating a new Conda environment %s", id)

            conda_cli = os.path.join(conda_dir, "condabin", "conda")

            success = self._run_command(conda_cli, "env", "create", "--json", '-p', os.path.join(conda_dir, id), "--file", conda_file, output=output)

            if success and pip_file is not None:
                success = self._run_command(conda_cli, "install", "--json", '-p', os.path.join(conda_dir, id), "pip", "git", output=output)
                success = success and self.run("pip", "install", "-r", pip_file, output=output)

            if success and shell_file is not None:
                # TODO: make interpreter configurable
                success = self.run("/bin/bash", "--norc", "-e", shell_file, output=output)

        if success:
            return True
        else:
            shutil.rmtree(os.path.join(conda_dir, id))
            return False

    def _setup_source(self, repository, commit):
        with _lock_sources():

            sources_dir = get_config().sources

            source_hash = dict_hash({"repository" : repository, "commit" : commit})

            destination = os.path.join(sources_dir, source_hash)

            try:

                if os.path.isdir(os.path.join(destination, ".git")):
                    return destination

                logger.info("Cloning source code from %s (revision %s)", repository, commit)

                repo = git.Repo.clone_from(repository, destination, no_checkout=True)
                repo.git.checkout(commit)

                return destination
            except Exception as e:
                logger.error(e)
                return None

    def _run_command(self, *command, env=None, cwd=None, output=None, replace=False):

        envvars = dict(os.environ.items())
        
        envvars.pop("VIRTUAL_ENV", None) # Not using virtual env (just in case)
        envvars.pop("DISPLAY", None) # Disable X support

        if env is not None:
            envvars.update(env)

        envvars["PYTHONUNBUFFERED"] = "1" # Required to get any realtime output from Pyton

        logger.debug("Environment variables set: %s", envvars)

        logger.debug("Running in %s", cwd)

        if replace:
            os.execvpe(command[0], command, envvars)

        try:
            process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=envvars, cwd=cwd)

            while True:
                
                line = process.stdout.readline().decode()
                if line == "" and process.poll() is not None:
                    break
                if output is not None:
                    output(line)

            if output is not None:
                output(None)

        except KeyboardInterrupt:
            logger.info("Shutting down.")
            process.terminate()
            logger.info("Terminated by user.")

        return process.poll() == 0

    def _process_envvars(self, env=None):
        envvars = dict()
        envvars["PATH"] = os.environ.get("PATH", "")

        conda_dir = get_config().conda

        if self._conda_env is not None:
            conda_prefix = conda_dir
            if "CONDA_PREFIX" in os.environ:
                prefix = os.environ["CONDA_PREFIX"]
                envvars["PATH"] = os.path.pathsep.join([x for x in envvars["PATH"].split(os.path.pathsep) if not x.startswith(prefix)])

            envvars["PATH"] = os.path.join(conda_prefix, self._conda_env, "bin") + os.path.pathsep + envvars["PATH"]
            envvars["CONDA_PREFIX"] = os.path.join(conda_prefix, self._conda_env)
            envvars["CONDA_PYTHON_EXE"] = os.path.join(conda_prefix, self._conda_env, "bin", "python")
            envvars["CONDA_DEFAULT_ENV"] = os.path.join(conda_prefix, self._conda_env)

        if "VIRTUAL_ENV" in os.environ:
            prefix = os.environ["VIRTUAL_ENV"]
            envvars["PATH"] = os.path.pathsep.join([x for x in envvars["PATH"].split(os.path.pathsep) if not x.startswith(prefix)])

        envvars["PYTHONPATH"] = self._source_dir

        if env is not None:
            envvars.update(env)

        return envvars

    def shell(self):
        """Opens a shell in environment, replacing current process

        Returns:
            [type]: [description]
        """

        shell = os.environ.get("SHELL", "/bin/sh")

        self._run_command(shell, "--norc", cwd=self._source_dir, env=self._process_envvars(), replace=True)

    def run(self, *command, cwd=None, output=None, env=None):

        success = self._run_command(*command, cwd=cwd, env=self._process_envvars(env), output=output)

        return success

    def export(self, *command, cwd=None, env=None):

        envvars = self._process_envvars(env)

        script = "#!/bin/sh -e \n"

        for name, value in envvars.items():
            script += "export {}=\"{}\" \n".format(name, value.replace("\"", "\\\""))

        script += "\nexec {}\n".format(" ".join(command))

        return script

    @property
    def repository(self):
        return self._repository

    @property
    def commit(self):
        return self._commit

    @property
    def entrypoints(self):
        if self._entrypoints is not None:
            return self._entrypoints.entrypoints

        if not self.setup():
            raise RuntimeError("Unable to setup environment")

        entryfile = _find_file(self._source_dir, ["entrypoints.yaml", "entrypoints.yml", "entrypoints.py"])

        if entryfile is None:
            self._entrypoints = Entrypoints()
        else:

            debug = logger.isEnabledFor(logging.DEBUG)

            if entryfile.endswith(".py"):
                logger.debug("Generating entrypoints cache from %s", entryfile)
                cache = os.path.join(self._source_dir, "entrypoints.yaml")
                output = FileOutput(cache)

                if debug:
                    output = Multiplexer(output, PrintOutput())

                if not self.run("python", entryfile, cwd=self._source_dir, output=output):
                    print(output.contents())
                    os.remove(cache)
                    raise RuntimeError("Unable to obtain entrypoints due to script error.")
                entryfile = cache

            logger.debug("Loading entrypoints from %s", entryfile)

            self._entrypoints = Entrypoints.read(entryfile)

        return self._entrypoints.entrypoints
        