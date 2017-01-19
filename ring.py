from hashlib import sha1 
import sys


"""
    A class for navigating the keyspace of the DHT
"""

class Ring(object):

    def hashint(self, data):
        h = sha1(data)
        k = int(h.hexdigest(), 16)
        return k

    def find_closest_peer(self, keyname, peers, localinfo):
        kint = self.hashint(keyname)
        shortest_distance = int('FFFFFFFFFFFFFFFF', 16)
        shortest_distance_name = None
        peerscpy = peers[:]
        peerscpy.append(localinfo)
        for p in peerscpy:
            print p
            k = self.hashint(p)
            print "p: %d" % k
            distance = abs(k - kint)
            print "distance: %d " % distance
            if shortest_distance_name:
                if distance <= shortest_distance:
                    shortest_distance = distance
                    shortest_distance_name = p
            else:
                shortest_distance_name = p
                shortest_distance=distance
        print "Shortest distance is: %s " % shortest_distance_name
        return(shortest_distance_name)     



