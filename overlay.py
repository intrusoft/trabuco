import persist
import ring
import socket
import sys

"""
    Manage of swarm of peers nodes that will discover one another
"""

class Overlay(object):
    localsock = None
    peers = []

    def __init__(self, localip, localport):
        self.localip = localip
        self.localport = localport
        self.localnodename = "%s:%s" % (self.localip,self.localport)
        self.ring = ring.Ring()
        self.persistence = persist.Persist() 

    def listen(self):
        print "listen()"
        self.localsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.localsock.bind((self.localip, int(self.localport)))

    def join(self, peerip, peerport):
        print "join"
        msg = "joi%s:%s" % (str(self.localip), str(self.localport))
        print "Sending: %s " % msg
        self.localsock.sendto(msg, (peerip, int(peerport)))
        self.peers.append("%s:%s" % (peerip, int(peerport)))
        return True         

    def _migrate_key_away(self, node, key, value):
        (ip,port) = node.split(':')
        msg = "mgt%s:%s=%s" % (self.localnodename, key, value)
        print "Sending: %s" % msg
        self.localsock.sendto(msg, (ip, int(port)))
        

    def _rebalance(self, keys):
        # detect rebalance
        rebalancers = self.ring.detect_rebalance_keys(keys, self.localnodename, self.peers)
        # migrate rebalancers
        for k in rebalancers:
            keynode = self.ring.find_closest_peer(k, self.peers, self.localnodename)
            print "Migrating %s to %s " % (k, keynode)
            self._migrate_key_away(keynode, k, self.persistence.get(k))
            
            

    def _new_peer_connected(self, peerinfo):
        print "_new_peer_connected %s" % peerinfo
        if not peerinfo in self.peers:
            print "Adding peer to list of peers"
            self.peers.append(peerinfo)

        # send the peer that connected a list of current peers
        msg = "lst%s" % ",".join(self.peers)
        self.localsock.sendto(msg, (peerinfo.split(':')[0], int(peerinfo.split(':')[1])))        

        # NOTIFY the other peers about new list of peers
        for p in self.peers:
            (ip,port) = p.split(':')
            self.localsock.sendto(msg, (ip, int(port)))        

        # detect rebalance
        self._rebalance(self.persistence.getkeys())


        # something in the background to check peers health and individual lists are consistent
        # occasionally compare peer list with others and learn new hosts?

    def _update_peer_list(self, peerlist):
        for p in peerlist.split(','):
            localinfo = "%s:%s" % (self.localip, self.localport)
            if p != localinfo:
                if not p in self.peers:
                    self.peers.append(p)
        print "known peers: %s " % str(self.peers)

        # detect rebalance
        self._rebalance(self.persistence.getkeys())

    def _forward_set(self, node, keyname, value):
        (ip,port) = node.split(':')
        msg = "set%s=%s" % (keyname, value)
        print "Sending to %s " % node
        self.localsock.sendto(msg, (ip, int(port)))

    def _forward_get(self, node, clientinfo, keyname):
        (ip,port) = node.split(':')
        msg = "fgt%s:%s" % (clientinfo, keyname) # This is problematic
        print "Sending to %s " % node
        self.localsock.sendto(msg, (ip, int(port)))

    def _answer_get(self, clientinfo, keyname, value):
        (ip,port) = clientinfo.split(':')
        msg = "ANS:%s=%s" % (keyname, value)
        print "Sending: %s to %s " % (msg, clientinfo)
        self.localsock.sendto(msg, (ip, int(port))) 

    def _set_key(self, keyname, value):
        # keyspace class
        keynode = self.ring.find_closest_peer(keyname, self.peers, self.localnodename)
        print "Chosen node for this key is: %s " % keynode
        # check if its the node I'm on, if it is record, if not forward to that node.
        if keynode == self.localnodename:
            print "I SHOULD HOST THIS DATA!"
            self.persistence.set(keyname, value)
        else:
            print "I NEED TO FORWARD THIS"        
            self._forward_set(keynode, keyname, value)

        # pass in peers and their hashes
        # find which peer is the "cloest" to hash of keyname
        # do I just straight to the peer I think? or do I traverse left or right to find it?
        pass

    def _get_key(self, keyname, clientinfo):
        keynode = self.ring.find_closest_peer(keyname, self.peers, self.localnodename)
        if keynode == self.localnodename:
            print "I HAVE THIS, I'LL FETCH IT"
            value = self.persistence.get(keyname)
            print "value from persistence engine: %s " % value
            self._answer_get(clientinfo, keyname, value)
        else:
            print "I DONT HAVE THIS, FORWARD TO SOMEBODY WHO DOES"
            self._forward_get(keynode, clientinfo, keyname)

    def _delete_remote_key(self, node, keyname):
        (ip,port) = node.split(':')
        msg = "drk%s" % keyname
        print "Sending %s to %s " % (msg, node)
        self.localsock.sendto(msg, (ip, int(port)))

    def _handle_migrated_key(self, old_node, key, value):
        self._set_key(key, value)
        # delete key from old node
        self._delete_remote_key(old_node, key)

    def _delete_local_key(self, key):
        self.persistence.delete(key)   

    def _handle_request(self, data, clientinfo):
        print "_handle_request()"
        print data
        verb = data[:3]
        if verb == 'joi':
            self._new_peer_connected(data[3:])
        if verb == 'not':
            self._new_peer_notifcation(data[3:])
        if verb == 'lst':
            self._update_peer_list(data[3:])
        if verb == 'set':
            (keyname,value) = data[3:].split('=')
            self._set_key(keyname, value)
        if verb == 'get':
            self._get_key(data[3:], clientinfo)
        if verb == 'fgt': #forwarded get, data node can respond directly to client
            payload = data[3:]
            original_client = "%s:%s" % (payload.split(':')[0], payload.split(':')[1])
            keyname = payload.split(':')[2]
            self._get_key(keyname, original_client)
        if verb == 'mgt':
            payload = data[3:]
            old_node = "%s:%s" % (payload.split(':')[0], payload.split(':')[1])
            kv = payload.split(':')[2]
            (key,value) = kv.split('=')
            self._handle_migrated_key(old_node, key, value)
        if verb == 'drk':
            key = data[3:]
            self._delete_local_key(key)

    def participate(self):
        print "participate()"
        if self.localsock:
            while True:
                print "known peers: %s " % str(self.peers)
                print "recvfrom"
                data, addr = self.localsock.recvfrom(1024)
                print "received: %s" % data
                self._handle_request(data, ":".join((addr[0],str(addr[1]))))                                
        else:
            print "No existing socket listening..."
        

