import sys
import os
import json

from attributee import Attribute, Attributee
from attributee.primitives import String, to_string

from msks import logger

from re import Pattern, compile

class Pattern(Attribute):

    def coerce(self, value, ctx):
        if value is None:
            return None
        if isinstance(value, Pattern):
            return value
        value = to_string(value)
        return compile(value)

    def dump(self, value):
        if value is None:
            return None

        return value.pattern


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

class LogProcessor(LogHandler):

    @property
    def data(self):
        return {"type": "none"}

class FileWriter(object):

    def __call__(self, content):
        pass

class WriterLogProcessor(LogHandler):

    def __init__(self, writer=None) -> None:
        super().__init__()
        self._writer = writer

    @property
    def writer(self):
        return self._writer

    def _save(self):
        if self.writer is not None:
            self.writer(json.dumps(self.data))

    @property
    def data(self):
        return {"type": "none"}

class MeasuresAggregator(Attributee, WriterLogProcessor):

    measure = Pattern(default="([a-zA-Z0-9_-]+) *: *(.*)")

    def __init__(self, *args, writer=None, **kwargs):
        super().__init__(*args, **kwargs)
        WriterLogProcessor.__init__(self, writer)
        self._data = {}

    def __call__(self, line: str):
        
        if line is None:
            self._save()
            return

        line = line.strip("\n")

        match = self.measure.match(line)

        if match is not None:
            name = match.group(1)
            value = match.group(2)
            self._addmeasure(name, value)

    def _addmeasure(self, name, value):
        try:
            value = float(value)
        except ValueError:
            pass

        self._data[name] = value

    @property
    def data(self):
        return {"type": "aggregated", "data": self._data}

class IterativeMeasuresAggregator(Attributee, WriterLogProcessor):

    step = Pattern(default="step: *([0-9]+)")
    measure = Pattern(default="([a-zA-Z0-9_-]+) *: *(.*)")

    def __init__(self, *args, writer=None, **kwargs):
        super().__init__(*args, **kwargs)
        WriterLogProcessor.__init__(self, writer)
        self._step = None
        self._offset = None
        self._data = []

    def __call__(self, line: str):
        
        if line is None:
            self._save()
            return

        line = line.strip("\n")

        match = self.step.match(line)

        if match is not None:
            step = int(match.group(1))
            self._makestep(step)
            return

        match = self.measure.match(line)

        if match is not None:
            name = match.group(1)
            value = match.group(2)
            self._makestep()
            self._addmeasure(name, value)

    def _makestep(self, step=None):
    
        if self._step is None:
            if step is None:
                step = 0

            self._step = step
            self._offset = step
        else:
            if not step is None and self._step < step:
                self._step = step

        if len(self._data) >= self._step - self._offset + 1:
            return

        self._data += [{}] * (self._step - self._offset + 1 - len(self._data))

        self._save()

    def _addmeasure(self, name, value):
    
        i = self._step - self._offset

        try:
            value = float(value)
        except ValueError:
            pass

        self._data[i][name] = value

    @property
    def data(self):
        return {"type": "iterative", "offset": self._offset, "steps": self._data}
