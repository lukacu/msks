
import os
from pathlib import Path

from attributee import Attributee, String, Map, Nested
from attributee.io import Serializable

_default_root = os.path.join(os.environ["HOME"], ".local", "msks")
class Registry(Attributee):
    url = String(default=os.environ.get("MSKS_REGISTRY", None), readonly=True)
    key = String(default=None)
    
class Configuration(Attributee, Serializable):

    registry = Nested(Registry)
    conda = String(default=os.environ.get("MSKS_CONDA_DIR", os.path.join(_default_root, "conda")), readonly=True)
    sources = String(default=os.environ.get("MSKS_SOURCES_DIR", os.path.join(_default_root, "sources")), readonly=True)
    tasks = String(default=os.environ.get("MSKS_TASKS_DIR", os.path.join(_default_root, "tasks")), readonly=True)
    aliases = Map(String(), default={})

_HOME = str(Path.home())
_CONFIG = None

def get_config() -> Configuration:
    global _CONFIG

    if _CONFIG is not None:
        return _CONFIG

    configfile = os.path.join(_HOME, ".config", "msks", "config.yaml")

    if os.path.isfile(configfile):
        _CONFIG = Configuration.read(configfile)
    else:
        _CONFIG = Configuration()

    return _CONFIG

