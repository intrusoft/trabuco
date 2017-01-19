#!/usr/bin/env python

import argparse
import socket
import sys
import time

"""

GET and SET keys/values in the DHT using a simple UDP protocol

- Provide an IP and port for one of the nodes in the DHT
- Specify get or set as the command and provide details of key
- The sim command will create 250 key/values using the format fooN=barN where N is in the range of 0-250


"""


def main(peerip, peerport, cmd, key, value):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    if cmd.lower() == 'get':        
        sock.sendto("get%s" % key, (peerip, int(peerport)))
        (data, addr) = sock.recvfrom(256)
        print data
    if cmd.lower() == 'set':
        sock.sendto("set%s=%s" % (key,value), (peerip, int(peerport)))
        print "Sent.."
    if cmd.lower() == 'sim':
        for x in range(250):
            sock.sendto("setfoo%s=bar%s" % (x,x), (peerip, int(peerport)))
            print x
            time.sleep(.01)

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DHT Query')
    parser.add_argument('--peerport', default=10001)
    parser.add_argument('--peerip', default='127.0.0.1')
    parser.add_argument('--cmd', default='get')
    parser.add_argument('--key', default=None)
    parser.add_argument('--value', default=None)
    args = parser.parse_args()
    print "Usage: ./query.py -key foo --value bar\n\n"
    main(args.peerip, args.peerport, args.cmd, args.key, args.value)





