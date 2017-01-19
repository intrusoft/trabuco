
"""
    Class for handling the keys of the node
    For now we are just using an in memory dictionary, but could be extended to persist to disk
"""
class Persist(object):
    data = {}

    def set(self, keyname, value):
        self.data[keyname] = value
        print "contents of data is now: %s" % str(self.data)
        print "keys: %d" % len(self.data.keys())

    def get(self, keyname):
        if keyname in self.data.keys():
            return self.data[keyname]
        else:
            return None 

    def delete(self, keyname):
        if keyname in self.data.keys():
            del self.data[keyname]

    def getkeys(self):
        return self.data.keys()


