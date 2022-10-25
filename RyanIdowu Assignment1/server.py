# based on https://pythontic.com/modules/socket/udp-client-server-example
# Author: Ryan Idowu
from socket import *
import packetOperations
NUMBER_OF_WORKERS = 3

localIP = "server"
localPort = 50000
bufferSize = 1024

ack = str.encode('OK: Received request')

# Contents of each worker
workerContents = [['a.txt', 'b.txt', 'c.txt'],
                  ['d.txt', 'e.txt', 'f.txt'],
                  ['g.txt', 'h.txt', 'i.txt']]
workersIP = {}
# 0: ('worker', 50000),
# 1: ('worker1', 50000),
# 2: ('worker2', 50000)

# Create a datagram socket
UDPServerSocket = socket(AF_INET, SOCK_DGRAM)
# Bind to address and ip
UDPServerSocket.bind((localIP, localPort))

print("UDP server up and listening")
# Get worker IP
for i in range(NUMBER_OF_WORKERS):
    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    workerNum = bytesAddressPair[0].decode()
    workersIP[workerNum] = bytesAddressPair[1]
    print("Worker: {} registered!".format(workerNum))

# Listen for incoming datagrams
while True:
    # Receive request from client
    clientRequest = UDPServerSocket.recvfrom(bufferSize)
    address = clientRequest[1]
    name, seqNum, filename = packetOperations.depacketize(clientRequest)
    print("Message from Client:{}".format(filename))
    # Send ack to client
    # UDPServerSocket.sendto(ack, address)
    # Find worker with file
    workerIndex = 'null'
    for files in workerContents:
        if filename in files:
            workerIndex = str(workerContents.index(files))
            print(workerIndex)
    if workerIndex != 'null':
        worker = workersIP[workerIndex]
        # Send request to worker
        resend = True
        bytesToSend = packetOperations.packetize("Server", 1, filename)
        UDPServerSocket.sendto(bytesToSend, worker)
        while resend:
            # Sequencing and Timeouts
            try:
                # Set timeout for retransmission of request
                UDPServerSocket.settimeout(10)
                # Receive packet from worker
                packetFromWorker = UDPServerSocket.recvfrom(bufferSize)
                resend = False
                UDPServerSocket.settimeout(None)
                sender, workerSeqNum, data = packetOperations.depacketize(packetFromWorker)
                print(data)
                seqNum = 0
                f = open("Server {}".format(filename), 'w')
                acknowledge = packetOperations.packetize("Server", seqNum,
                                                         "received packet {} expecting packet {} ".format(
                                                             seqNum , seqNum + 1))
                # Receive file from worker
                while data:
                    if data == 'end':
                        print("File Downloaded")
                        acknowledge = packetOperations.packetize("Server", seqNum + 1,
                                                                 "received packet {}  ".format(
                                                                     seqNum))
                        UDPServerSocket.sendto(acknowledge, worker)
                        f.close()
                        break
                    # Check if packet received is expected packet
                    if sender != "Client":
                        if int(workerSeqNum) == seqNum:
                            f.write(data)
                            seqNum += 1
                            # Send ack with next expected packet
                            acknowledge = packetOperations.packetize("Server", seqNum,
                                                                     "received packet {} expecting packet {} ".format(
                                                                         seqNum - 1, seqNum))
                            UDPServerSocket.sendto(acknowledge, worker)
                    packetFromWorker = UDPServerSocket.recvfrom(bufferSize)
                    sender, workerSeqNum, data = packetOperations.depacketize(packetFromWorker)
            # Worker doesnt send result before timeout - resend request
            except timeout:
                UDPServerSocket.sendto(bytesToSend, worker)
    else:
        f = open("Server {}".format(filename), 'w')
        f.write("Error file not found")
    # Send file to Client
    f = open("Server {}".format(filename), 'r')
    data = f.read(bufferSize-24)
    toSend = packetOperations.packetize("Server", 0, data)
    UDPServerSocket.sendto(toSend, address)
    clientSeqNum = 0
    UDPServerSocket.settimeout(2)
    while int(clientSeqNum) < int(seqNum) + 1:
        try:
            # Receive ack from client
            packetFromClient = UDPServerSocket.recvfrom(bufferSize)
            name, clientSeqNum, message = packetOperations.depacketize(packetFromClient)
            if int(clientSeqNum) > int(seqNum) + 1:
                break
            if int(clientSeqNum) == int(seqNum):
                toSend = packetOperations.packetize("Server", clientSeqNum, "end")
                UDPServerSocket.sendto(toSend, address)
            else:
                toSend = packetOperations.packetize("Server", clientSeqNum, data)
                print(message)
                UDPServerSocket.sendto(toSend, address)
                print("sending ...")
                data = f.read(bufferSize-24)
        except timeout:
            if clientSeqNum == seqNum:
                break
            UDPServerSocket.sendto(toSend, address)
            print("timeout resending ...")
    # Send end packet to server
    UDPServerSocket.settimeout(None)
    print("Sent")
