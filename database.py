from threading import Thread
import os
import time
import datetime
from inmemory_data import Data
import shutil

class Database(Data):
    def __init__(self, snapshotdir, timegap, keptfile = 30):
        Data.__init__(self, snapshotdir, timegap, keptfile)
        thread = Thread(target = self._takesnapshot)
        thread.daemon = True
        thread.start()

    def _loadcheckpoint(self):
        snapshotfiles = sorted(os.listdir(self._spdir),  reverse=True, key=lambda f:os.path.getctime(os.path.join(self._spdir, f)))
        latestfile = None
        for f in snapshotfiles:
            if f.startswith('checkpoint_'):
                latestfile = f
                break
        if latestfile == None:
            print("No snapshot file")
            return

        self._loadcheckpointfile(os.path.join(self._spdir, latestfile))

    def _loadtransaction(self):

        self._spmutex.acquire()
        transactiondir = os.path.join(self._spdir, 'transaction')
        if not os.path.exists(transactiondir):
            print("no transaction file")

        else:
            transactionfiles = sorted(os.listdir(transactiondir),  key=lambda f:os.path.getctime(os.path.join(transactiondir, f)))
            filesnum = len(transactionfiles)
            latestfile = None
            if filesnum > 1:
                print('too many transaction file, pick the latest one')
            if filesnum>0:
                latestfile = transactionfiles[0]
            if latestfile == None:
                print("no transaction file")

            self._loadtransactionfile(os.path.join(transactiondir, latestfile))
            os.remove(os.path.join(transactiondir,latestfile))

        self._spmutex.release()

    def _takesnapshot(self):
        while True:
            self._spmutex.acquire()
            start_time = time.time()
            filename = "checkpoint_{0}".format(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
            snapshot = os.path.join(self._spdir,filename)
            f = open(snapshot, 'w')
            for key in self._data.keys():
                f.write("{0} {1} {2}\n".format(key, self._data[key][0], self._data[key][1]))
            f.close()
            snapshotfiles = sorted(os.listdir(self._spdir),  key=lambda f:os.path.getctime(os.path.join(self._spdir, f)))
            if len(snapshotfiles) > self._keptfile:
                i = 0
                while i<len(snapshotfiles) - self._keptfile:
                    if not os.path.isfile(os.path.join(self._spdir,snapshotfiles[i])):
                        i += 1
                        continue
                    os.remove(os.path.join(self._spdir,snapshotfiles[i]))
                    i+=1

            self._spmutex.release()
            current_time = time.time()
            if current_time-start_time<self._timegap:
                time.sleep(self._timegap-current_time+start_time)

    def gettransactionpath(self):
        return os.path.join(self._spdir, r'transaction')

    def commit(self):
        self._loadtransaction()
        filename = "checkpoint_{0}".format(datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S"))
        snapshot = os.path.join(self._spdir,filename)
        f = open(snapshot, 'w')
        for key in self._data.keys():
            f.write("{0} {1} {2}\n".format(key, self._data[key][0], self._data[key][1]))
        f.close()
