import socket
import sys
import time

def main(argv):

    for x in range(10000):
        msg = 'setkey%d=foo' % x
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(msg, ('127.0.0.1', 2001))
        print "sent %s " % msg
        time.sleep(.01)

if __name__ == '__main__':
    main(sys.argv)

