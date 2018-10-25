#include <mutex>
#include <queue>
#include <chrono>
#include <atomic>
#include <vector>
#include <thread>
#include <random>
#include <fstream>
#include <errno.h>
#include <assert.h>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>

using namespace std;

#define MAX_NODES 30
#define MAX_LENGTH 100
#define FALSE 0
#define TRUE 1
int startPort;
atomic<int> totalReceivedMessages = ATOMIC_VAR_INIT(0);
atomic<long long int> totalResponseTime = ATOMIC_VAR_INIT(0);

typedef chrono::time_point<chrono::system_clock> Time;

enum messageType
{
    PRIVILEGE,
    REQUEST,
    TERMINATE
};

typedef struct Message
{
    enum messageType type;
    int senderID;
    int requestSequenceNumber;
    int LN[MAX_NODES];
    char Q[MAX_LENGTH]; // process-identifiers delimited by comma
} Message;

/*
 * Socket Utility Functions
 */
int createReceiveSocket(int myID, int myPort)
{
    printf("INFO :: Node %d: createReceiveSocket -> Creating Socket %hu\n", myID, myPort);
    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(myPort);
    bzero(&server.sin_zero, 0);

    // creating the recv socket
    int sockfd;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in creating socket :: Reason %s\n", myID, strerror(errno));
        exit(EXIT_FAILURE);
    }

    // binding TCP server to IP and port
    if (bind(sockfd, (struct sockaddr *)&server, sizeof(struct sockaddr_in)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in binding socket to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        exit(EXIT_FAILURE);
    }

    // mark it for listening
    if (listen(sockfd, MAX_NODES))
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in listening to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        exit(EXIT_FAILURE);
    }

    // set socket timeout
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in creating timeout for TCP connection :: Reason %s\n", myID, strerror(errno));
        exit(EXIT_FAILURE);
    }
    return sockfd;
}

bool sendMessage(int myID, int dstID, Message &message)
{
    assert(myID != dstID);

    // creating the send socket
    int sockfd;
    if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        printf("ERROR :: Node %d: sendMessage -> Error in creating send socket :: Reason - %s\n", myID, strerror(errno));
        close(sockfd);
        return false;
    }

    struct sockaddr_in server;
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    server.sin_port = htons(startPort + dstID);
    bzero(&server.sin_zero, 8);

    // connecting TCP to server
    if (connect(sockfd, (struct sockaddr *)(&server), sizeof(struct sockaddr_in)) < 0)
    {
        printf("ERROR :: Node %d: sendMessage -> Error in connecting to server %d:: Reason - %s\n", myID, dstID, strerror(errno));
        close(sockfd);
        return false;
    }

    if (send(sockfd, (char *)(&message), sizeof(message), 0) < 0)
    {
        printf("WARN :: Node %d: sendMessage -> Error in sending message-type %s to %d :: Reason - %s\n", myID, ((message.type == PRIVILEGE) ? "PRIVILEGE" : "REQUEST"), dstID, strerror(errno));
        close(sockfd);
        return false;
    }

    close(sockfd);
    return true;
}

/*
 * Token Manipulation Functions 
 */

bool isIdPresentInQueue(int id, char Q[])
{
    char strId[10];
    sprintf(strId, "%d", id);
    return ((strstr(Q, strId) == NULL) ? false : true);
}

void enqueue(int id, char Q[])
{
    char strId[10];
    if(strlen(Q) == 0)
    {
        sprintf(strId, "%d", id);
    } else
    {
        sprintf(strId, ",%d", id);
    }
    char *ptr = strcat(Q, strId);
    strcpy(Q, ptr);
}

int dequeue(char Q[])
{
    string str = string(Q);
    assert(str.length() > 0);
    int id;
    string newString = "";
    size_t index = str.find(",");
    if (index != string::npos)
    {
        id = stoi(str.substr(0, index));
        newString = str.substr(index + 1);
    }
    else
    {
        id = stoi(str);
    }
    strcpy(Q, newString.c_str());
    return id;
}

/*
    Suzuki Kasami Algorithm
    For function descriptions of enterCS , exitCS, receiveMessage
    refer https://dl.acm.org/citation.cfm?id=214406
*/
void requestCS(int myID, int &requesting, int &havePrivilege, vector<int> &RN,
               int noOfNodes, Time &start, FILE *fp, mutex *lock)
{
    lock->lock();

    requesting = TRUE;
    long long int sysTime;

    if (havePrivilege == FALSE)
    {
        // sending request to all other nodes
        RN[myID]++;

        Message message;
        message.senderID = myID;
        message.type = REQUEST;
        message.requestSequenceNumber = RN[myID];
        lock->unlock();

        for (int i = 0; i < noOfNodes; i++)
        {
            if (i != myID)
            {
                sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
                fprintf(fp, "%d sends REQUEST to %d with SN %d at %lld\n", myID, i, RN[myID], sysTime);
                fflush(fp);
                if (sendMessage(myID, i, message) == false)
                {
                    printf("ERROR :: Node %d: requestCS -> Unable to send REQUEST to %d\n", myID, i);
                    exit(EXIT_FAILURE);
                }
            }
        }
    }
    else
    {
        lock->unlock();
    }

    while (true)
    {
        lock->lock();
        // wait for privilege
        if (havePrivilege == TRUE)
        {
            lock->unlock();
            break;
        }
        lock->unlock();
        // this_thread::sleep_for(chrono::milliseconds(10));
    }
}

void exitCS(int myID, int &requesting, int &havePrivilege, vector<int> &RN,
            Message **sharedTokenPtrPtr, int noOfNodes, Time &start, FILE *fp, mutex *lock)
{
    lock->lock();

    if (*sharedTokenPtrPtr == NULL)
    {
        printf("ERROR :: Node %d: exitCS -> sharedTokenPtrPtr points to NULL\n", myID);
        exit(EXIT_FAILURE);
    }
    (*sharedTokenPtrPtr)->LN[myID] = RN[myID];

    for (int i = 0; i < noOfNodes; i++)
    {
        if (i != myID)
        {
            // add pending nodes in token queue 
            if (isIdPresentInQueue(i, (*sharedTokenPtrPtr)->Q) == false && RN[i] == (*sharedTokenPtrPtr)->LN[i] + 1)
            {
                enqueue(i, (*sharedTokenPtrPtr)->Q);
            }
        }
    }

    if (strlen((*sharedTokenPtrPtr)->Q) > 0)
    {
        int nextNode = dequeue((*sharedTokenPtrPtr)->Q);

        long long int sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d sends PRIVILEGE to %d with queue |%s| at %lld\n", myID, nextNode, (*sharedTokenPtrPtr)->Q, sysTime);
        fflush(fp);

        // sending token to next in queue
        if (sendMessage(myID, nextNode, **sharedTokenPtrPtr) == false)
        {
            printf("ERROR :: Node %d: exitCS -> Could not send PRIVILEGE to %d\n", myID, nextNode);
            exit(EXIT_FAILURE);
        }
        havePrivilege = FALSE;
        *sharedTokenPtrPtr = NULL;
    }
    requesting = FALSE;

    lock->unlock();
}

void working(int myID, int &havePrivilege, int &requesting, vector<int> &RN, atomic<int> &finishedProcessesCount,
            Message **sharedTokenPtrPtr, int noOfNodes, int mutualExclusionCounts, float alpha, float beta, Time &start, FILE *fp, mutex *lock)
{
    default_random_engine generatorLocalComputation;
    default_random_engine generatorCSComputation;
    exponential_distribution<double> distributionLocalComputation(1 / alpha);
    exponential_distribution<double> distributionCSComputation(1 / beta);

    int sendAmount;
    long long int sysTime;
    Time requestCSTime;

    printf("INFO :: Node %d: working -> Starting Critical Section Simulations\n", myID);

    for (int i = 1; i <= mutualExclusionCounts; i++)
    {
        int outCSTime = distributionLocalComputation(generatorLocalComputation);
        int inCSTime = distributionCSComputation(generatorCSComputation);

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d is doing local computation at %lld\n", myID, sysTime);
        fflush(fp);
        sleep(outCSTime);

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d requests to enter CS for the %dst time at %lld\n", myID, i, sysTime);
        fflush(fp);

        requestCSTime = chrono::system_clock::now();
        requestCS(myID, requesting, havePrivilege, RN, noOfNodes, start, fp, lock);
        totalResponseTime += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - requestCSTime).count();

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d ENTERS CS for the %dst time at %lld\n", myID, i, sysTime);
        fflush(fp);
        sleep(inCSTime);

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d EXITS CS for the %dst time at %lld\n", myID, i, sysTime);
        fflush(fp);
        exitCS(myID, requesting, havePrivilege, RN, sharedTokenPtrPtr, noOfNodes, start, fp, lock);
    }
    sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
    fprintf(fp, "%d completed all %d transactions at %lld\n", myID, mutualExclusionCounts, sysTime);
    fflush(fp);

    // node is alive as long as all processes have finished
    // other nodes might need routing of privilege across completed nodes

    // informing all other nodes of termination
    Message terminateMessage;
    terminateMessage.senderID = myID;
    terminateMessage.type = TERMINATE;

    for(int i = 0; i < noOfNodes; i++)
    {
        if(i != myID) 
        {
            sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
            fprintf(fp, "%d sends TERMINATE to %d at %lld\n", myID, i, sysTime);
            fflush(fp);
            if (sendMessage(myID, i, terminateMessage) == false)
            {
                printf("ERROR :: Node %d: working -> Could not send TERMINATE to %d\n", myID, i);
                exit(EXIT_FAILURE);
            }
        }
    }

    finishedProcessesCount++;
    while (finishedProcessesCount < noOfNodes)
    {
        lock->lock();
        if (havePrivilege == true)
        {
            lock->unlock();
            exitCS(myID, requesting, havePrivilege, RN, sharedTokenPtrPtr, noOfNodes, start, fp, lock);
        }
        else
        {
            lock->unlock();            
        }
        // this_thread::sleep_for(chrono::milliseconds(10));
    }
    sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
    fprintf(fp, "%d finished any pending transactions at %lld\n", myID, sysTime);
    fflush(fp);
}

void receiveMessage(int myID, int myPort, int &havePrivilege, int &requesting, vector<int> &RN, atomic<int> &finishedProcessesCount,
                    Message **sharedTokenPtrPtr, int noOfNodes, Time &start, FILE *fp, mutex *lock)
{
    struct sockaddr_in client;
    socklen_t len = sizeof(struct sockaddr_in);

    Message message;
    Message token;

    Time now;
    int clientId;
    long long int sysTime;

    int sockfd = createReceiveSocket(myID, myPort);

    printf("INFO :: Node %d: receiveMessage -> Started listening for connections\n", myID);
    while (finishedProcessesCount < noOfNodes)
    {
        if ((clientId = accept(sockfd, (struct sockaddr *)&client, &len)) >= 0)
        {
            // receiving message from client
            int data_len = recv(clientId, (char *)&message, sizeof(message), 0);
            if (data_len > 0)
            {
                now = chrono::system_clock::now();
                totalReceivedMessages++;
                switch (message.type)
                {
                case PRIVILEGE:
                    sysTime = chrono::duration_cast<chrono::microseconds>(now - start).count();
                    fprintf(fp, "%d receives PRIVILEGE from %d with queue |%s| at %lld\n", myID, message.senderID, message.Q, sysTime);
                    fflush(fp);

                    lock->lock();

                    havePrivilege = TRUE;
                    token = message;
                    token.senderID = myID;
                    *sharedTokenPtrPtr = &token;    // to share token with sender thread

                    lock->unlock();
                    break;
                case REQUEST:
                    sysTime = chrono::duration_cast<chrono::microseconds>(now - start).count();
                    fprintf(fp, "%d received REQUEST from %d with SN %d at %lld\n", myID, message.senderID, message.requestSequenceNumber, sysTime);
                    fflush(fp);

                    lock->lock();

                    RN[message.senderID] = max(RN[message.senderID], message.requestSequenceNumber); // handle outdated requests

                    if (havePrivilege == TRUE && requesting == FALSE)
                    {
                        if (*sharedTokenPtrPtr == NULL)
                        {
                            printf("ERROR :: Node %d: receiveMessage -> sharedTokenPtrPtr points to NULL\n", myID);
                            exit(EXIT_FAILURE);
                        }

                        if (RN[message.senderID] == (*sharedTokenPtrPtr)->LN[message.senderID] + 1)
                        {
                            sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
                            fprintf(fp, "%d sends PRIVILEGE to %d with queue |%s| at %lld\n", myID, message.senderID, (*sharedTokenPtrPtr)->Q, sysTime);
                            fflush(fp);

                            // sending token to requesting process
                            if (sendMessage(myID, message.senderID, **sharedTokenPtrPtr) == false)
                            {
                                printf("ERROR :: Node %d: receiveMessage -> Could not send PRIVILEGE to %d\n", myID, message.senderID);
                                exit(EXIT_FAILURE);
                            }
                            havePrivilege = FALSE;
                            *sharedTokenPtrPtr = NULL;
                        }
                    }

                    lock->unlock();
                    break;
                case TERMINATE:
                    sysTime = chrono::duration_cast<chrono::microseconds>(now - start).count();
                    fprintf(fp, "%d received TERMINATE from %d at %lld\n", myID, message.senderID, sysTime);
                    fflush(fp);
                    finishedProcessesCount++;
                    break;
                default:
                    printf("ERROR :: Node %d: receiveMessage -> Invalid Message Type %d\n", myID, message.type);
                }
            }
        }
    }
    sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
    fprintf(fp, "%d stopped receiving threads at %lld\n", myID, sysTime);
    fflush(fp);
}

void run(int noOfNodes, int mutualExclusionCounts, int initialTokenNode, float alpha, float beta)
{
    fclose(fopen("output.txt", "w")); // remove file contents
    FILE *fp = fopen("output.txt", "a+");

    Time start = chrono::system_clock::now();

    vector<thread> workerSenders(noOfNodes);
    vector<thread> workerReceivers(noOfNodes);

    vector<int> havePrivilege(noOfNodes, FALSE);
    havePrivilege[initialTokenNode] = TRUE;

    vector<int> requesting(noOfNodes, FALSE);

    vector<vector<int>> RN(noOfNodes, vector<int>(noOfNodes, -1));
    vector<Message **> sharedTokenPtrPtr(noOfNodes, NULL);
    vector<Message *> sharedTokenPtr(noOfNodes, NULL);

    vector<atomic<int> > finishedProcessesCount(noOfNodes);

    for (int i = 0; i < noOfNodes; i++)
    {
        sharedTokenPtrPtr[i] = &sharedTokenPtr[i];
        finishedProcessesCount[i] = ATOMIC_VAR_INIT(0);
    }

    vector<mutex> locks(noOfNodes);

    Message tokenObj;
    tokenObj.senderID = initialTokenNode;
    for (int i = 0; i < MAX_NODES; i++)
    {
        tokenObj.LN[i] = -1;
    }
    string emptyString = "";
    strcpy(tokenObj.Q, emptyString.c_str());

    sharedTokenPtr[initialTokenNode] = &tokenObj;

    printf("Creating receiver threads\n");

    // starting the receiver threads
    for (int i = 0; i < noOfNodes; i++)
    {
        workerReceivers[i] = thread(receiveMessage, i, startPort + i,
                                    ref(havePrivilege[i]), ref(requesting[i]), ref(RN[i]), ref(finishedProcessesCount[i]),
                                    sharedTokenPtrPtr[i], noOfNodes, ref(start), fp, &locks[i]);
    }
    this_thread::sleep_for(chrono::seconds(1));

    printf("Creating CS executor threads\n");

    // starting the sender threads
    for (int i = 0; i < noOfNodes; i++)
    {
        workerSenders[i] = thread(working, i,
                                  ref(havePrivilege[i]), ref(requesting[i]), ref(RN[i]), ref(finishedProcessesCount[i]),
                                  sharedTokenPtrPtr[i], noOfNodes, mutualExclusionCounts, alpha, beta, ref(start), fp, &locks[i]);
    }

    for (int i = 0; i < noOfNodes; i++)
    {
        workerSenders[i].join();
        workerReceivers[i].join();
    }
    float averageMessagesExchanged = totalReceivedMessages / (noOfNodes * 1.0);
    float averageResponseTime = totalResponseTime / (1000.0 * noOfNodes * mutualExclusionCounts);
    cout << "\n\nAnalysis:\n\tTotal Messages Exchanged:  " << totalReceivedMessages << "\n\tAverage Messages Exchanged: " << averageMessagesExchanged << endl;
    cout << "\n\tAverage Response Time: " << averageResponseTime << " milliseconds" << endl;
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
        mutualExclusionCounts,
        initialTokenNode;
    float alpha,
        beta;

    fin >> noOfNodes >> mutualExclusionCounts >> initialTokenNode >> alpha >> beta;
    fin.close();
    run(noOfNodes, mutualExclusionCounts, initialTokenNode, alpha, beta);

    return 0;
}