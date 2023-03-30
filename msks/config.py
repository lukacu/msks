
import os
from pathlib import Path

from attributee import Attributee, String, Map, Nested, Object, List, URL
from attributee.io import Serializable

_default_root = os.path.join(os.environ["HOME"], ".local", "msks")


class Configuration(Attributee, Serializable):

    store = URL(default="file://" + os.path.join(_default_root, "tasks"))
    cache = String(default=os.environ.get("MSKS_CACHE_DIR", os.path.join(_default_root, "cache")), readonly=True)
    aliases = Map(String(), default={})
    notify = List(Object(subclass="msks.notify.Channel"), default={})

    @property
    def conda(self):
        return os.path.join(self.cache, "conda")

    @property
    def sources(self):
        return os.path.join(self.cache, "sources")

    @property
    def runtime(self):
        return os.path.join(self.cache, "runtime")

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

