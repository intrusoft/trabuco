import socket
import sys

def main(argv):
    key = argv[1]
    print "Query: %s" % key

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto(key, ('127.0.0.1', 2001))
    if key[:3] == 'get':
        (data, addr) = sock.recvfrom(256)
        print data

if __name__ == '__main__':
    main(sys.argv)

