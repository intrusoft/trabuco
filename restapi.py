
from flask import Flask, request
import socket

"""

Basic demonstration of putting a ReST API in front of the DHT
TODO: Refactor so that services can share same UDP client code and not each have to do direct socket stuff

"""


app = Flask(__name__)

localip = '127.0.0.1'
localport = 10001

@app.route("/trabuco/keys/<key>", methods=['GET'])
def getkey(key):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.sendto("get%s" % key, (localip, int(localport)))
    (data, addr) = sock.recvfrom(256)
    return data[4:]

@app.route("/trabuco/keys/<key>", methods=['POST','PUT'])
def setkey(key):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    for k in request.form.keys():
        msg = "set%s=%s" % (k, request.form[k])
        print "Sending: %s " % msg
        sock.sendto(msg, (localip, int(localport)))
    return ""

if __name__ == "__main__":
    app.run()
