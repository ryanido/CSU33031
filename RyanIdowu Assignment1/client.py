# based on https://pythontic.com/modules/socket/udp-client-server-example
# Author: Ryan Idowu
from socket import *
import packetOperations

serverAddressPort = ("server", 50000)
bufferSize = 1024

# Create a UDP socket at client side
UDPClientSocket = socket(AF_INET, SOCK_DGRAM)

print('Client running')
while True:
    msgFromClient = input('Enter request file')
    # Send request to Server
    resend = True
    bytesToSend = packetOperations.packetize("Client", 1, msgFromClient)
    UDPClientSocket.sendto(bytesToSend, serverAddressPort)
    while resend:
        try:
            # Set timeout for retransmission of request
            UDPClientSocket.settimeout(10)
            # Receive packet from server
            packetFromServer = UDPClientSocket.recvfrom(bufferSize)
            resend = False
            UDPClientSocket.settimeout(None)
            sender, serverSeqNum, data = packetOperations.depacketize(packetFromServer)
            seqNum = 0
            f = open("Client {}".format(msgFromClient), 'w')
            acknowledge = packetOperations.packetize("Client", seqNum,
                                                     "received packet {} expecting packet {} ".format(
                                                         seqNum, seqNum + 1))
            while data:
                if data == 'end':
                    print("File Downloaded")
                    f.close()
                    acknowledge = packetOperations.packetize("Client", seqNum + 1,
                                                             "received packet {}".format(
                                                                 seqNum))
                    UDPClientSocket.sendto(acknowledge, serverAddressPort)
                    break
                # Check if packet sent is expected packet
                if int(serverSeqNum) == seqNum:
                    f.write(data)
                    seqNum += 1
                    # Send ack with expected packet
                    acknowledge = packetOperations.packetize("Client", seqNum,
                                                             "received packet {} expecting packet {} ".format(
                                                                 seqNum - 1, seqNum))
                    UDPClientSocket.sendto(acknowledge, serverAddressPort)
                packetFromServer = UDPClientSocket.recvfrom(bufferSize)
                name, serverSeqNum, data = packetOperations.depacketize(packetFromServer)
        # Server doesnt send result before timeout - resend request
        except timeout:
            print("resending")
            UDPClientSocket.sendto(bytesToSend, serverAddressPort)
