from threading import Thread
import os
import time
import datetime
from inmemory_data import Data
from transaction import transaction
import shutil

RECORD = True
class BlockData(Data):
    def __init__(self, snapshotdir, timegap, keptfile = 30):
        self._transactionsdir = snapshotdir
        self._transactions = [[]]
        self._level = 0
        self._commit = 0
        Data.__init__(self, snapshotdir, keptfile, timegap)
        self._transactionsfile = os.path.join(self._transactionsdir, "transaction{0}".format(self._commit))
        open(self._transactionsfile, 'a').close()


    def _appendtran(self, action):
        f = open(self._transactionsfile,'a')
        f.write("{0} {1}\n".format(action.__str__(), self._level))
        f.close()

    def setvalue(self, action):

        self._block._appendtran(action)
        self._data[action.key]=(action.value, action.version)
        while self._level >= len(self._transactions):
            self._transactions.append([])

        self._transactions[self._level].append(action)
        self._updatevaluescount(action)

    def _loadtransaction(self):
        self._spmutex.acquire()
        if not os.path.exists(self._transactionsdir):
            print("no transaction file")
        else:
            transactionsfiles = sorted(os.listdir(self._transactionsdir),  key=lambda f:os.path.getctime(os.path.join(self._transactionsdir, f)))
            for f in transactionsfiles[:-1]:
                if not os.path.isfile(os.path.join(self._transactionsdir, f)):
                    continue
                for line in open(os.path.join(self._transactionsdir, f)).read().splitlines():
                    segments = line.split(" ")
                    key = segments[0]
                    values = (segments[1], segments[2], segments[3])
                    self._data[segments[0]]=(segments[1], segments[2],)

            if len(transactionsfiles) > 0:
                self._loadtransactionfile(os.path.join(self._transactionsdir, transactionsfiles[-1]))
                self._commit = len(transactionsfiles)-1
            else:
                self._commit = len(transactionsfiles)
        self._spmutex.release()

    def commit(self, dstpath):
        if not os.path.exists(dstpath):
            os.makedirs(dstpath)
        if not os.path.exists(os.path.join(dstpath, 'latest')):
            shutil.copy(self._transactionsfile, os.path.join(dstpath, 'latest'))
        for f in os.listdir(self._transactionsdir):
            os.remove(os.path.join(self._transactionsdir, f))
        self._commit = 0
        self._transactionsfile = os.path.join(self._transactionsdir, "transaction{0}".format(self._commit))
        open(self._transactionsfile, 'a').close()
        self._values.clear()
        self._removedvalues.clear()
        self._transactions = [[]]
        self._level = 0


    def rollback(self):
        temp_transactions_list = self._transactions[self._level]
        for tran in reversed(temp_transactions_list):
            action = transaction(tran.key, (tran.oldvalue, tran.version,tran.value))
            self._appendtran(action)
            self.setvalue(action)
        if self._level == 0:
            self._transactions = [[]]
        else:
            self._transactions.pop()

        if self._level > 0:
            self._level -= 1
        if 0 == len(temp_transactions_list):
            return "NO Transaction"
        return "Success"

    def begin(self, dstpath):
        if not os.path.exists(dstpath):
            os.makedirs(dstpath)
        if not os.path.exists(os.path.join(os.path.join(dstpath, 'latest'))):
            shutil.copy(self._transactionsfile, os.path.join(self._spdir, os.path.join(dstpath, 'latest')))
        self._commit += 1
        self._transactionsfile = os.path.join(self._transactionsdir, "transaction{0}".format(self._commit))
        open(self._transactionsfile, 'a').close()
        self._level += 1
        self._values.clear()
        self._removedvalues.clear()
        self._transactions.append([])

    def end(self):
         for f in os.listdir(self._transactionsdir):
            os.remove(os.path.join(self._transactionsdir, f))

    def gettransactionpath(self):
        return self._transactionsfile

