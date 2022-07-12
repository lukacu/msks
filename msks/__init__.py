

import logging
import json
import hashlib

logger = logging.getLogger("msks")

__version__ = "0.0.1"

def dict_hash(dictionary) -> str:
    dhash = hashlib.sha1()
    encoded = json.dumps(dictionary, sort_keys=True).encode()
    dhash.update(encoded)
    return dhash.hexdigest()
