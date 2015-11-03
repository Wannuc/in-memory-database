from threading import Thread
import time
import datetime
import os
import abc
from transaction import transaction
import shutil
import thread
class Data:

    def __init__(self, snapshotdir, timegap, keptfile = 30):
        self._spdir = snapshotdir
        self._data = {}
        self._timegap = timegap
        self._keptfile = keptfile
        self._values = {}
        self._removedvalues = {}
        self._spmutex = thread.allocate_lock()
        if not os.path.exists(self._spdir):
            os.makedirs(self._spdir)
        self._loadcheckpoint()
        self._loadtransaction()

    def _updatevaluescount(self, action):
        oldvalue = action.oldvalue
        value = action.value
        if value in self._values.keys():
            self._values[value] += 1
        else:
            self._values[value] = 1

        if oldvalue in self._removedvalues.keys():
            self._removedvalues[oldvalue] += 1
        else:
            self._removedvalues[oldvalue] = 1

    def _loadtransactionfile(self, filename):
        for line in open(filename).read().splitlines():
            segments = line.split(" ")
            key = segments[0]
            values = (segments[1], segments[2], segments[3])
            action = transaction(key, values)
            self._data[segments[0]]=(segments[1], segments[2],)
            self._updatevaluescount(action)

    def _loadcheckpointfile(self, filename):
        for line in open(filename).read().splitlines():
            segments = line.split(" ")
            key = segments[0]
            values = (segments[1], segments[2], None)
            action = transaction(key, values)
            self._data[segments[0]]=(segments[1], segments[2],)
            self._updatevaluescount(action)

    @abc.abstractmethod
    def _loadcheckpoint(self):
        return

    @abc.abstractmethod
    def _loadtransaction(self):
        return

    @abc.abstractmethod
    def _takesnapshot(self):
        return

    def clear(self):
        self._spmutex.acquire()
        self._data = {}
        self._values = {}
        self._removedvalues = {}
        if os.path.exists(self._spdir):
            shutil.rmtree(self._spdir)
        self._spmutex.release()

    def getvalue(self, key):
        if key in self._data.keys():
            return self._data[key]
        return None

    def getversion(self, key):
        if key in self._data.keys():
            return self._data[key][1]

        return 0

    def getcount(self, key):
        remain = 0
        if key in self._values.keys():
            remain = self._values[key]
        removed = 0
        if key in self._removedvalues.keys():
            removed = self._removedvalues[key]
        return remain - removed