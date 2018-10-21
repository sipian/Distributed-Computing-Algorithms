#include <mutex>
#include <vector>
#include <thread>
#include <random>
#include <time.h>
#include <climits>
#include <fstream>
#include <stdio.h>
#include <sstream>
#include <iostream>
#include <string.h>
#include <stdlib.h>

#include "SocketUtilities.cpp"

using namespace std;

typedef struct Locks
{
    mutex transactionLock;
} Locks;

template <class T>
T randomElement(vector<T> &arr)
{
    auto begin = arr.begin();
    auto end = arr.end();
    int n = std::distance(begin, end);
    int pos = rand() % n;
    std::advance(begin, pos);
    return *begin;
}


/*
 *
 * 
 *  Bank Transaction Functions
 * 
 * 
*/
void simulateTransaction(int myID, float lambda, int *balance, int *netTransferredAmt, bool *terminate, Locks *locks, Time &start, FILE *fp, Graph *topology)
{
    default_random_engine generator;
    exponential_distribution<double> distribution(1 / lambda);

    Message message;
    message.type = APPLICATION_MESSAGE;
    message.senderId = myID;

    int sendAmount;
    Time now;
    long long int sysTime;

    printf("Starting Sending Simulations for Process #%d\n", myID);
    while (*terminate == false)
    {
        sleep(distribution(generator));

        int sendtoBank = randomElement<int>(topology->neighbors[myID]);

        locks->transactionLock.lock();

        if (*balance <= 0)
        {
            goto transactionLockUnlockLabel;
        }

        sendAmount = 1 + (rand() % static_cast<int>(*balance));
        message.transactionAmount = sendAmount;
        message.dstID = sendtoBank;
        now = chrono::system_clock::now();
        if (sendMessage(myID, message, topology))
        {
            // update values only if send succeeds
            *balance -= sendAmount;
            *netTransferredAmt += sendAmount;
            sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
            fprintf(fp, "Process#%d sends Rs. %d to Process#%d at %lld\n", myID, sendAmount, sendtoBank, sysTime);
            printf("INFO :: simulateTransaction -> :: Process#%d sent Rs. %d to Process#%d\n", myID, sendAmount, sendtoBank);
            fflush(fp);
        }

    transactionLockUnlockLabel:
        locks->transactionLock.unlock();
    }
    printf("Process#%d STOPPING SENDING\n", myID);
}

void receiveMessage(int receiveSockfd, int myID, int *balance, int *netTransferredAmt, bool *terminate, Locks *locks, Time &start, FILE *fp, Graph *topology)
{
    struct sockaddr_in client;
    socklen_t len = sizeof(struct sockaddr_in);

    Message message;
    int sequenceNumber = 0,
        expectedSequenceNumber = 1,
        numMarkerReceived = 0,
        numNeighbors = topology->neighbors[myID].size(),
        clientId,
        sentMarkerMessagesCount = 0,
        receivedMarkerMessagesCount = 0;
    Time now;
    long long int sysTime;

    vector<int> receivedAlongChannel(topology->V, 0);
    SnapshotMessage snapshotMessage;
    string channelState = "";

    printf("INFO :: Node %d: receiveMessage -> Starting listening for connections\n", myID);
    while (true)
    {
        if ((clientId = accept(receiveSockfd, (struct sockaddr *)&client, &len)) >= 0)
        {
            // receiving message from client
            int data_len = recv(clientId, (char *)&message, sizeof(message), 0);
            if (data_len > 0)
            {
                now = chrono::system_clock::now();
                switch (message.type)
                {
                case APPLICATION_MESSAGE:
                    if (message.dstID == myID)
                    {
                        locks->transactionLock.lock();
                        *balance += message.transactionAmount;
                        if (expectedSequenceNumber == sequenceNumber)
                        {
                            // snapshot collection is going on
                            receivedAlongChannel[message.senderId] += message.transactionAmount;
                        }
                        sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
                        printf("INFO :: receiveMessage -> Process#%d receives Rs. %d from Process#%d\n", myID, message.transactionAmount, message.senderId);
                        fprintf(fp, "Process#%d receives Rs. %d from Process#%d at %lld\n", myID, message.transactionAmount, message.senderId, sysTime);
                        fflush(fp);
                        locks->transactionLock.unlock();
                    }
                    else
                    {
                        printf("WARN :: Node %d: receiveMessage -> Received Application Message with invalid dstID:#%d\n", myID, message.dstID);
                    }
                    break;

                case MARKER_MESSAGE:

                    receivedMarkerMessagesCount++;

                    if (expectedSequenceNumber != sequenceNumber)
                    {
                        locks->transactionLock.lock();
                        snapshotMessage.balance = *balance;
                        snapshotMessage.netAmountTransferred = *netTransferredAmt;
                        sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
                        printf("INFO :: Node %d: receiveMessage from %d -> Takes its local snapshot\n", myID, message.senderId);
                        fprintf(fp, "Process#%d takes its local snapshot at %lld\n", myID, sysTime);
                        // Format "channel-number,sentMessages,receivedMessages\n"
                        channelState = to_string(message.senderId) + ",0,0\n";

                        // not considering coordinator as neighbor
                        if (message.senderId > 0)
                        {
                            numMarkerReceived++;
                        }
                        // resetting the channel states
                        fill(receivedAlongChannel.begin(), receivedAlongChannel.end(), 0);
                        sequenceNumber = expectedSequenceNumber;
                        if (broadcastMessageToNeighbors(myID, MARKER_MESSAGE, start, fp, topology) == false)
                        {
                            exit(1);
                        }
                        sentMarkerMessagesCount += numNeighbors;
                        locks->transactionLock.unlock();
                    }
                    else
                    {
                        // not considering coordinator as neighbor
                        if (message.senderId > 0)
                        {
                            channelState += to_string(message.senderId) + ",0," + to_string(receivedAlongChannel[message.senderId]) + "\n";
                            numMarkerReceived++;
                        }
                    }
                    if (numMarkerReceived >= numNeighbors)
                    {
                        printf("INFO :: Node %d: receiveMessage -> Received All markers from neighbors\n", myID);
                        strcpy(snapshotMessage.snapshotChannelState, channelState.c_str());
                        if (sendSnapShotToCoordinator(myID, SNAPSHOT_MESSAGE, snapshotMessage, start, fp, topology) == false)
                        {
                            exit(1);
                        }
                        expectedSequenceNumber++;
                        numMarkerReceived = 0;
                    }
                    break;

                case SNAPSHOT_MESSAGE:
                    forwardMessage(myID, &message, start, fp, topology);
                    break;

                case MESSAGE_COMPLEXITY:
                    forwardMessage(myID, &message, start, fp, topology);
                    break;

                case INITIATE_TERMINATION_MESSAGE:
                    // stopping sender process
                    if (*terminate == false)
                    {
                        *terminate = true;

                        printf("INFO :: Node %d: receiveMessage -> RECEIVED INITIATE_TERMINATION_MESSAGE\n", myID);

                        if (broadcastMessageToNeighbors(myID, INITIATE_TERMINATION_MESSAGE, start, fp, topology) == false)
                        {
                            exit(1);
                        }
                        snapshotMessage.sentMarkerMessagesCount = sentMarkerMessagesCount;
                        snapshotMessage.receivedMarkerMessagesCount = receivedMarkerMessagesCount;
                        if (sendSnapShotToCoordinator(myID, MESSAGE_COMPLEXITY, snapshotMessage, start, fp, topology) == false)
                        {
                            exit(1);
                        }
                    }
                    break;

                case TERMINATE_MESSAGE:
                    broadcastMessageToNeighbors(myID, TERMINATE_MESSAGE, start, fp, topology);
                    printf("Process#%d STOPPING RECEIVING\n", myID);
                    return;

                default:
                    printf("ERROR :: Node %d: receiveMessage -> INVALID message type\n", myID);
                    exit(1);
                }
            }
        }
    }
}

std::vector<std::string> tokenize(std::string const &str, const char delim)
{
    // construct a stream from the string
    std::stringstream ss(str);

    std::vector<std::string> out;

    std::string s;
    while (std::getline(ss, s, delim))
    {
        out.push_back(s);
    }
    return out;
}

int parseSnapShotChannelState(const char *channelState)
{
    vector<string> channelStates = tokenize(string(channelState), '\n');
    int channelStateBalance = 0;
    for (string s : channelStates)
    {
        if (s.size() > 0)
        {

            vector<string> values = tokenize(s, ',');
            if (values.size() != 3)
            {
                printf("ERROR :: parseSnapShotChannelState -> Incorrect number of values in token %lu from string %s\n", values.size(), s.c_str());
                exit(2);
            }
            if (values[2] != "0")
            {
                printf("\n\nWOOOOAAAHHHH :: Channel State for %s is non zero : %s\n\n", values[0].c_str(), values[2].c_str());
            }
            channelStateBalance += stoi(values[2]);
        }
    }
    return channelStateBalance;
}

void receiveMessageForCoordinator(int sockfd, int myID, int *totalAmountTransferred, bool *snapShotReceived, Time &start, FILE *fp, Graph *topology)
{
    struct sockaddr_in client;
    socklen_t len = sizeof(struct sockaddr_in);
    Message message;
    int counter = 1,
        totalReceivedBalance = 0,
        clientId;
    Time now;
    long long int sysTime;
    int numSnapshots = 0;

    int sentMarkerMessagesCount = 0;
    int receivedMarkerMessagesCount = 0;

    while (true)
    {
        if ((clientId = accept(sockfd, (struct sockaddr *)&client, &len)) >= 0)
        {
            // receiving message from client
            int data_len = recv(clientId, (char *)&message, sizeof(message), 0);
            if (data_len > 0)
            {
                now = chrono::system_clock::now();
                switch (message.type)
                {
                case SNAPSHOT_MESSAGE:
                    printf("\nSnapshot Collected from %d :: Balance : %d , TransferredAmount : %d\n", message.senderId, message.snapshotMessage.balance, message.snapshotMessage.netAmountTransferred);
                    fprintf(fp, "Coordinator receives snapshot from %d %lld\n", message.senderId, sysTime);
                    totalReceivedBalance += message.snapshotMessage.balance + parseSnapShotChannelState(message.snapshotMessage.snapshotChannelState);
                    *totalAmountTransferred += message.snapshotMessage.netAmountTransferred;
                    numSnapshots++;

                    if (numSnapshots == topology->V - 1)
                    { // do not consider coordinator for snapshot
                        printf("\n\n\n ------ \n\n\nSUUCCCESSSS #%d :: Snapshot Collected Balance :: %d, Net Amount Transferred :: %d\n\n\n ------ \n\n\n", counter, totalReceivedBalance, *totalAmountTransferred);
                        sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
                        fprintf(fp, "Coordinator receives all snapshots %lld\n", sysTime);
                        totalReceivedBalance = 0;
                        numSnapshots = 0;
                        counter++;
                        *snapShotReceived = true;
                    }
                    break;

                case MESSAGE_COMPLEXITY:
                    printf("\nMessage Complexity Collected from %d :: Sent Marker Messages : %d , Received Marker Messages : %d\n", message.senderId, message.snapshotMessage.sentMarkerMessagesCount, message.snapshotMessage.receivedMarkerMessagesCount);
                    fprintf(fp, "Coordinator receives message-complexity snapshot from %d %lld\n", message.senderId, sysTime);
                    numSnapshots++;
                    sentMarkerMessagesCount += message.snapshotMessage.sentMarkerMessagesCount;
                    receivedMarkerMessagesCount += message.snapshotMessage.receivedMarkerMessagesCount;

                    if (numSnapshots == topology->V - 1)
                    { // do not consider coordinator for snapshot
                        printf("\n\n\n ------ \n\n\nEND SUCCESS :: Message Complexity :: Total Sent Marker Messages Count : %d , Total Received Marker Messages Count : %d\n\n\n ------ \n\n\n", sentMarkerMessagesCount, receivedMarkerMessagesCount);
                        sysTime = std::chrono::duration_cast<std::chrono::microseconds>(now - start).count();
                        fprintf(fp, "Coordinator receives all message-complexity snapshots %lld\n", sysTime);
                        broadcastMessageToNeighbors(myID, TERMINATE_MESSAGE, start, fp, topology);
                        return;
                    }
                    break;
                default:
                    printf("WARN :: Node %d: receiveMessageForCoordinator -> Received Non Snapshot Message (Type:: %d) from %d in receiving data\n", myID, message.type, message.senderId);
                }
            }
        }
    }
}

void runCoordinator(int maxTransferredAmount, int sockfd, Time &start, FILE *fp, Graph *topology)
{
    int myID = 0;
    int myPort = topology->portMapping[myID];

    int *totalAmountTransferred = new int;
    *totalAmountTransferred = 0;

    bool *snapShotReceived = new bool;
    *snapShotReceived = false;

    auto threadRecv = thread(receiveMessageForCoordinator, sockfd, myID, totalAmountTransferred, snapShotReceived, ref(start), fp, topology);

    while (true)
    {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        if (broadcastMessageToNeighbors(myID, MARKER_MESSAGE, start, fp, topology) == false)
        {
            exit(1);
        }

        while (*snapShotReceived == false)
        {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        if (*totalAmountTransferred > maxTransferredAmount)
        {
            printf("\n\n\n ++++++++++ \n\n\n%d > %d :: Time to stop\n\n\n ++++++++++ \n\n\n", *totalAmountTransferred, maxTransferredAmount);
            if (broadcastMessageToNeighbors(myID, INITIATE_TERMINATION_MESSAGE, start, fp, topology) == false)
            {
                exit(1);
            }
            break;
        }
        else
        {
            printf("\n\n\n ++++++++++ \n\n\n%d <= %d :: Work Still Left\n\n\n ++++++++++ \n\n\n", *totalAmountTransferred, maxTransferredAmount);
        }
        *snapShotReceived = false;
    }
    threadRecv.join();
}

int runBank(int myID, int initialBalance, float lambda, int sockfd, Time &start, FILE *fp, Graph *topology)
{
    int *balance = new int;
    int *netTransferredAmt = new int;
    bool *terminate = new bool;
    Locks *locks = new Locks;

    *balance = initialBalance;
    *netTransferredAmt = 0;
    *terminate = false;

    auto threadRecv = thread(receiveMessage, sockfd, myID, balance, netTransferredAmt, terminate, locks, ref(start), fp, topology);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    simulateTransaction(myID, lambda, balance, netTransferredAmt, terminate, locks, start, fp, topology);
    threadRecv.join();
}

vector<int> startServers(Graph *topology)
{
    vector<int> bankSockets(topology->V);

    for (int ID = 0; ID < topology->V; ID++)
    {
        bankSockets[ID] = createReceiveSocket(ID, topology->routingTable[ID][ID]);
        if (bankSockets[ID] < 0)
        {
            printf("ERROR :: Node %d: startServers -> Error in creating receiver sockets for port %hu\n", ID, ntohs(topology->routingTable[ID][ID]));
        }
    }
    return bankSockets;
}

void run(float lambda, int initialBalance, int maxTransferredAmount, Graph *topology)
{
    fclose(fopen("output.txt", "w")); // remove file contents
    FILE *fp = fopen("output.txt", "a+");

    Time start = chrono::system_clock::now();

    vector<int> bankSockets = startServers(topology);
    vector<thread> banks(topology->V - 1);
    for (int i = 1; i < topology->V; i++)
    {
        banks[i - 1] = thread(runBank, i, initialBalance, lambda, bankSockets[i], ref(start), fp, topology);
    }
    thread coordinator = thread(runCoordinator, maxTransferredAmount, bankSockets[0], ref(start), fp, topology);

    for (int i = 1; i < topology->V; i++)
    {
        banks[i - 1].join();
    }
    coordinator.join();
    for (int &socket : bankSockets)
    {
        close(socket);
    }
}

int main(int argc, char *argv[])
{

    if (argc < 2)
    {
        cout << "\033[1;31mMissing input file path in arguments\033[0m\n";
        exit(EXIT_FAILURE);
    }
    ifstream fin(argv[1]);

    if (!fin)
    {
        cout << "\033[1;31mError In Opening inp-params.txt\033[0m\n";
        exit(EXIT_FAILURE);
    }

    if (argc < 3)
    {
        startPort = 10000;
    }
    else
    {
        startPort = atoi(argv[2]);
    }

    srand(time(NULL));

    //reading necessary details from the FILE
    int noOfNodes,
        initialBankBalance,
        maxTransferredAmount,
        lambda;

    fin >> noOfNodes >> initialBankBalance >> maxTransferredAmount >> lambda;

    Graph topology(noOfNodes);

    string list;
    vector<vector<int>> topo(noOfNodes, vector<int>(0));
    while (!fin.eof())
    {
        getline(fin, list);
        if (list.size() > 0)
        {
            istringstream ss(list);
            string word;
            ss >> word;
            // convert 1-indexing to 0-indexing
            int nodeId = stoi(word);
            while (true)
            {
                ss >> word;
                if (!ss)
                {
                    break;
                }
                topology.AddEdge(nodeId, stoi(word));
            }
        }
    }

    fin.close();

    topology.ConstructRoutingTable();
    topology.PrintRoutingTable();
    run(lambda, initialBankBalance, maxTransferredAmount, &topology);

    return 0;
}