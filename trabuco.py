"""

Simple DHT over simple UDP messaging

"""

import argparse
import overlay

def main(localip, localport, peerip, peerport):
    print "Main()"
    ov = overlay.Overlay(localip, localport)
    ov.listen()

    if peerip:
        ov.join(peerip, peerport)
    else:
        print "No peer info provided, assuming this is first node in cluster"

    ov.participate()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='DHT Demo')
    parser.add_argument('--localport', default='10001')
    parser.add_argument('--peerport', default=None)
    parser.add_argument('--localip', default='127.0.0.1')
    parser.add_argument('--peerip', default=None)
    args = parser.parse_args()
    main(args.localip, args.localport, args.peerip, args.peerport)


