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
            #print p
            k = self.hashint(p)
            #print "p: %d" % k
            distance = abs(k - kint)
            #print "distance: %d " % distance
            if shortest_distance_name:
                if distance <= shortest_distance:
                    shortest_distance = distance
                    shortest_distance_name = p
            else:
                shortest_distance_name = p
                shortest_distance=distance
        #print "Shortest distance is: %s " % shortest_distance_name
        return(shortest_distance_name)     

    def detect_rebalance_keys(self, localkeys, localinfo, peers):
        rebalancers = []
        print "Iterate through local keys %s " % str(localkeys)
        for k in localkeys:
            closest = self.find_closest_peer(k, peers, localinfo)
            print closest
            print localinfo
            if closest == localinfo:
                print "Key %s is still closest to local (%s)" % (k, localinfo)
            else:
                print "Key %s should be rebalanced to %s " % (k, closest)
                rebalancers.append(k)
        return rebalancers

if __name__ == "__main__":
    r = Ring()
    peers = ['1.1.1.1:100', '2.2.2.2:200', '3.3.3.3:300', '4.4.4.4:400', '5.5.5.5:500']
    rebalancers = r.detect_rebalance_keys(['a','b','c','d','e'], '1.1.1.1:100', peers)
    print "These need to be rebalanced %s" % str(rebalancers)
   

