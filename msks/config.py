
import os
from pathlib import Path

from attributee import Attributee, String, Map
from attributee.io import Serializable

class Configuration(Attributee, Serializable):

    sources = Map(String())

_CONFIG = None

def get_config() -> Configuration:
    global _CONFIG

    if _CONFIG is not None:
        return _CONFIG

    homedir = str(Path.home())

    configfile = os.path.join(homedir, ".config", "msks", "config.yaml")

    if os.path.isfile(configfile):
        _CONFIG = Configuration.read(configfile)
    else:
        _CONFIG = Configuration()

    return _CONFIG

