import sys
import os
import json

from attributee import Attribute, Attributee, Pattern
from attributee.primitives import String, to_string

from msks import logger

class LogHandler(object):

    def __call__(self, line):
        pass
    
class Multiplexer(LogHandler):

    def __init__(self, *processors):
        self._processors = processors

    def __call__(self, line):
        for processor in self._processors:
            try:
                processor(line)
            except Exception as e:
                logger.exception(e)

class FileOutput(LogHandler):

    def __init__(self, file):
        self._file = file
        self._handle = None

    def __call__(self, line):
        if self._handle is None:
            self._handle = open(self._file, "w")

        if line is not None:
            self._handle.write(line)
            self._handle.flush()
        else:
            self._handle.close()
            self._handle = None

    def contents(self):
        return open(self._file, "r").read()

class PrintOutput(LogHandler):

    def __call__(self, line):
        if line is not None:
            print(line, end='')
            sys.stdout.flush()

class LogProcessor(Attributee):

    delimiter = String(default=":")

    def handler(self, writer) -> LogHandler:
        pass

    @property
    def data(self):
        return {"type": "none"}

class ScoresExtractor(LogProcessor):

    def handler(self, writer):
        from re import compile
        data = {}
        pattern = compile("([a-zA-Z0-9_-]+) *{} *(-?[0-9\.]+)".format(self.delimiter))

        def callback(line: str):
            if line is None:                
                writer({"type": "aggregated", "data": data})
                return

            line = line.strip("\n")

            match = pattern.match(line)

            if match is not None:
                name = match.group(1)
                value = match.group(2)
                
                try:
                    value = float(value)
                except ValueError:
                    pass

                data[name] = value

        return callback

class StepsExtractor(LogProcessor):

    step = String(default="step")

    def handler(self, writer):
        from re import compile
        _data = []
        _state = {"offset": None, "step": None}

        step_pattern = compile("{} *{} *([0-9]+)".format(self.step, self.delimiter))
        value_pattern = compile("([a-zA-Z0-9_-]+) *{} *(-?[0-9\.]+)".format(self.delimiter))

        def dostep(step = None):

            if _state["step"] is None:
                if step is None: step = 0
                _state["step"] = step
                _state["offset"] = step
            else:
                if not step is None and _state["step"] < step:
                    _state["step"] = step
            if len(_data) >= _state["step"] - _state["offset"] + 1:
                return

            _data.extend([{}] * (_state["step"] - _state["offset"] + 1 - len(_data)))
            writer({"type": "steps", "offset": _state["offset"], "data": _data})

        def callback(line: str):
            if line is None:
                writer({"type": "steps", "offset": _state["offset"], "data": _data})
                return

            line = line.strip("\n")

            match = step_pattern.match(line)

            if match is not None:
                step = int(match.group(1))
                dostep(step)
                return

            match = value_pattern.match(line)

            if match is not None:
                name = match.group(1)
                value = match.group(2)
                dostep()
                
                i = _state["step"] - _state["offset"]

                try:
                    value = float(value)
                except ValueError:
                    pass

                _data[i][name] = value

        return callback

class SequencesExtractor(LogProcessor):

    def handler(self, writer):
        from re import compile
        _data = {}

        pattern = compile("([a-zA-Z0-9_-]+) *{} *(-?[0-9\.]+)".format(self.delimiter))

        def callback(line: str):
            if line is None:
                writer({"type": "sequences", "data": _data})
                return

            line = line.strip("\n")

            match = pattern.match(line)

            if match is not None:
                name = match.group(1)
                value = match.group(2)
                
                try:
                    value = float(value)
                except ValueError:
                    pass
                if name not in _data:
                    _data[name] = []
                _data[name].append(value)

        return callback

