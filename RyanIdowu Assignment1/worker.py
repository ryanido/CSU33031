from socket import *

# localIP = "127.0.0.2"  Worker
# localPort = 50000
import sys

import packetOperations
# Author: Ryan Idowu

bufferSize = 1024
serverAddressPort = ("server", 50000)
ack = 'OK: Received data'.encode()


class Worker:
    def __init__(self, num):
        self.num = num
        bytesToSend = str.encode(str(num))
        # Create a udp socket at worker side
        self.workerSocket = socket(AF_INET, SOCK_DGRAM)
        # Send address to server
        self.workerSocket.sendto(bytesToSend, serverAddressPort)
        print("UDP worker{} up and listening".format(num))


    def listen(self):
        # Listen for incoming datagrams
        while True:
            # Receive request from Server
            serverRq = self.workerSocket.recvfrom(bufferSize)
            name, seqNum, filename = packetOperations.depacketize(serverRq)
            address = serverRq[1]
            print('Message from Server {}'.format(filename))
            # Send ack to Server
            # self.workerSocket.sendto(ack, address)
            serverSeqNum = 0
            endSeq = 0
            f = open(filename, 'rb')
            data = f.read(bufferSize - 24)
            #Get length of file in sequence numbers
            while data:
                print(data)
                endSeq += 1
                print(endSeq)
                data = f.read(bufferSize-24)
            # Send file to Server
            f.close()
            f = open(filename, 'rb')
            data = f.read(bufferSize - 24)
            toSend = packetOperations.packetize("Worker", 0, data.decode())
            self.workerSocket.sendto(toSend, address)
            self.workerSocket.settimeout(2)
            while True:
                try:
                    # Receive ack from server
                    packetFromServer = self.workerSocket.recvfrom(bufferSize)
                    name, serverSeqNum, message = packetOperations.depacketize(packetFromServer)
                    print(message)
                    print("data: {}".format(data))
                    if int(serverSeqNum) > endSeq:
                        break
                    if int(serverSeqNum) == endSeq:
                        toSend = packetOperations.packetize("Worker", serverSeqNum, "end")
                        self.workerSocket.sendto(toSend, address)
                    else:
                        toSend = packetOperations.packetize("Worker", serverSeqNum, data.decode())
                        self.workerSocket.sendto(toSend, address)
                        print("sending ...")
                        data = f.read(bufferSize-24)
                except timeout:
                    if int(serverSeqNum) > endSeq:
                        break
                    self.workerSocket.sendto(toSend, address)
                    print("timeout resending ...")
            # Send end packet to server
            self.workerSocket.settimeout(None)
            print("Sent")


one = Worker(0)
one.listen()
