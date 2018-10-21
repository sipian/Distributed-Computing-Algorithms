#include <chrono>
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

#include "Graph.cpp"
using namespace std;
#define MAX_SNAPSHOT_CHANNEL_STATE_LENGTH 100

typedef chrono::time_point<chrono::system_clock> Time;

enum messageType
{
    MARKER_MESSAGE,
    APPLICATION_MESSAGE,
    TERMINATE_MESSAGE,
    INITIATE_TERMINATION_MESSAGE,
    SNAPSHOT_MESSAGE,
    MESSAGE_COMPLEXITY
};

typedef struct SnapshotMessage
{
    int balance;
    int netAmountTransferred;
    int sentMarkerMessagesCount;
    int receivedMarkerMessagesCount;
    char snapshotChannelState[MAX_SNAPSHOT_CHANNEL_STATE_LENGTH];
} SnapshotMessage;

typedef struct Message
{
    enum messageType type;
    int senderId;
    int dstID;
    int transactionAmount;
    SnapshotMessage snapshotMessage;
} message;

bool sendMessage(int myID, Message &message, Graph *topology)
{
    int dstID = message.dstID;
    // creating the recv socket
    int sockfd;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("WARN :: sendMessage -> Error in creating socket for node %d:: Reason - %s\n", myID, strerror(errno));
        return false;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    server.sin_port = topology->routingTable[myID][dstID];
    bzero(&server.sin_zero, 8);

    // connecting TCP to server
    if (connect(sockfd, (struct sockaddr *)(&server), sizeof(struct sockaddr_in)) < 0)
    {
        printf("WARN :: sendMessage -> Error in connecting to server for node %d:: Reason - %s\n", myID, strerror(errno));
        close(sockfd);
        return false;
    }

    if (send(sockfd, (char *)(&message), sizeof(message), 0) < 0)
    {
        printf("WARN :: Node %d: sendMessage -> Error in sending message-type %d to %d(%hu) :: Reason - %s\n", myID, message.type, message.dstID, ntohs(topology->routingTable[myID][dstID]), strerror(errno));
        close(sockfd);
        return false;
    }

    close(sockfd);
    return true;
}

bool broadcastMessageToNeighbors(int myID, messageType type, Time &start, FILE *fp, Graph *topology)
{
    Message message;
    message.type = type;
    message.senderId = myID;

    Time now;
    long long int sysTime;
    bool success = true;

    for (int nbr : topology->neighbors[myID])
    {
        message.dstID = nbr;
        now = chrono::system_clock::now();
        if (sendMessage(myID, message, topology))
        {
            sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
            fprintf(fp, "Process#%d sends Broadcast Message to Process#%d at %lld\n", myID, nbr, sysTime);
            fflush(fp);
            printf("INFO :: Node %d: broadcastMessageToNeighbors -> Sent Broadcast Message %d successfully to %d\n", myID, type, nbr);
        }
        else
        {
            success = false;
            printf("ERROR :: Node %d: broadcastMessageToNeighbors -> Could Not Send Broadcast Message %d to %d\n", myID, type, nbr);
        }
    }
    return success;
}

bool sendSnapShotToCoordinator(int myID, messageType type, SnapshotMessage &snapshot, Time &start, FILE *fp, Graph *topology)
{
    Message message;
    message.type = type;
    message.senderId = myID;
    // Node 0 is coordinator process
    message.dstID = 0;
    message.snapshotMessage = snapshot;
    uint16_t outputPort = topology->routingTable[myID][0];

    Time now = chrono::system_clock::now();
    if (sendMessage(myID, message, topology))
    {
        long long int sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
        fprintf(fp, "Process#%d sends SnapShot Message to Next-Hop-Router #%d at %lld\n", myID, topology->portNodeMapping[outputPort], sysTime);
        fflush(fp);
        printf("INFO :: Node %d: sendSnapShotToCoordinator -> Sent SnapShot Message successfully to Next-Hop-Router #%d\n", myID, topology->portNodeMapping[outputPort]);
    }
    else
    {
        printf("ERROR :: Node %d: sendSnapShotToCoordinator -> Could Not Send SnapShot Message to Next-Hop-Router #%d\n", myID, topology->portNodeMapping[outputPort]);
        return false;
    }
    return true;
}

bool forwardMessage(int myID, Message *message, Time &start, FILE *fp, Graph *topology)
{
    uint16_t outputPort = topology->routingTable[myID][message->dstID];

    Time now = chrono::system_clock::now();
    if (sendMessage(myID, *message, topology))
    {
        long long int sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
        fprintf(fp, "Process#%d forwards Message for (Src: #%d, Dest: #%d) to Next-Hop-Router #%d at %lld\n", myID, message->senderId, message->dstID, topology->portNodeMapping[outputPort], sysTime);
        fflush(fp);
        printf("INFO :: Node %d: forwardMessage -> Forwarded Message successfully for (Src: #%d, Dest: #%d) to Next-Hop-Router #%d\n", myID, message->senderId, message->dstID, topology->portNodeMapping[outputPort]);
    }
    else
    {
        printf("ERROR :: Node %d: forwardMessage -> Could not forward message for (Src: #%d, Dest: #%d) to Next-Hop-Router #%d\n", myID, message->senderId, message->dstID, topology->portNodeMapping[outputPort]);
        return false;
    }
    return true;
}

int createReceiveSocket(int myID, uint16_t myPort)
{
    printf("INFO :: Node %d: createReceiveSocket -> Creating Socket %hu\n", myID, ntohs(myPort));
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = myPort;
    bzero(&server.sin_zero, 0);

    // creating the recv socket
    int sockfd;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in creating socket :: Reason %s\n", myID, strerror(errno));
        return -1;
    }

    // binding TCP server to IP and port
    if (bind(sockfd, (struct sockaddr *)&server, sizeof(struct sockaddr_in)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in binding socket to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        return -1;
    }

    // mark it for listening
    if (listen(sockfd, MAX_NODES))
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in listening to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        return -1;
    }

    // set socket timeout
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in creating timeout for TCP connection :: Reason %s\n", myID, strerror(errno));
        return -1;
    }
    return sockfd;
}