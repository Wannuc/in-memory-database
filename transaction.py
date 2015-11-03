
class transaction:

    def __init__(self, key, values):
        self.key = key
        self.value = values[0]
        self.version = values[1]
        self.oldvalue = values[2]

    def __str__(self):
        return "{0} {1} {2} {3}".format(self.key, self.value, self.version, self.oldvalue)




