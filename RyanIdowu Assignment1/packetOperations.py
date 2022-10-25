# functions to make messages into 'packet' form
# Author: Ryan Idowu
def packetize(name, seqNum, message) :
    packet = name + "III" + str(seqNum) + "III" + message
    return str.encode(packet)


def depacketize(packet):
    packetMessage = packet[0].decode()
    name, seqNum, message = packetMessage.split("III")
    return name, seqNum, message