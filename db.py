from collections import OrderedDict
from database import Database
from inmemory_data import Data
from transaction import transaction
import time
import datetime
import os
from threading import Thread
from block import BlockData
RECORD = True
class InMemoryDb(object):

    "Simple in-memory database as a response to the Thumbtack coding challenge."

    def __init__(self, datadir, keptfile, timegap):
        "Initialize SimpleDb instance."
        self._db =  Database(os.path.join(datadir, 'db_snapshot'), 10)
        self._block = BlockData( os.path.join(datadir, r'block_snapshot'),10)

    def setvalue(self, key, value):
        values = self._block.getvalue(key)
        if values == None:
            values = self._db.getvalue(key)

        if values != None:
            values = (value, values[1], values[0])
        else:
            values = (value, 0, None)
        action = transaction(key, values)

        self._block.setvalue(action)

    def getvalue(self, key):
        value = self._block.getvalue(key)
        if value == None:
            value = self._db.getvalue(key)

        if value is None:
            return None
        return value[0]

    def commit(self):
        dst = self._db.gettransactionpath()
        self._block.commit(dst)
        self._db.commit()

    def count(self, key):
        return self._db.getcount(key) + self._block.getcount(key)

    def rollback(self):
        return self._block.rollback()

    def begin(self):
        dst = self._db.gettransactionpath()
        self._block.begin(dst)
        self._db.commit()

    def end(self):
        self._block.end()

    def clear(self):
        self._block.clear()
        self._db.clear()

def usage():
    print("Option:")

def run():
    "Reads commands from the command line and passes them through for processing."
    simpleDb = InMemoryDb(os.getcwd(), 30, 10)
    command = raw_input("Please Enter Command:\n")
    args = command.split(" ")
    while len(args) == 0 or args[0].upper() != "END":
        if len(args) != 0:
            if "SET" == args[0].upper():
                simpleDb.setvalue(args[1], args[2])
            elif "GET" == args[0].upper():
                print(simpleDb.getvalue(args[1]))
            elif "UNSET" == args[0].upper():
                simpleDb.setvalue(args[1], None)
            elif "COMMIT" == args[0].upper():
                simpleDb.commit()
            elif "NUMEQUALTO" == args[0].upper():
                print(simpleDb.count(args[1]))
            elif "ROLLBACK" == args[0].upper():
                print(simpleDb.rollback())
            elif "BEGIN" == args[0].upper():
                simpleDb.begin()
            elif "CLEAR" == args[0].upper():
                simpleDb.clear()
        else:
            usage()

        command = raw_input()
        args = command.split(" ")
    simpleDb.end()
    print("END")
    return

if __name__ == "__main__":
    run()