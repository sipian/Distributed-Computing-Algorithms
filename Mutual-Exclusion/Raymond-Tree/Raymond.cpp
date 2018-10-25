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
#define FALSE 0
#define TRUE 1
int startPort;
std::atomic<int> totalReceivedMessages = ATOMIC_VAR_INIT(0);
std::atomic<long long int> totalResponseTime = ATOMIC_VAR_INIT(0);

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
        exit(1);
    }

    // binding TCP server to IP and port
    if (bind(sockfd, (struct sockaddr *)&server, sizeof(struct sockaddr_in)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in binding socket to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        exit(1);
    }

    // mark it for listening
    if (listen(sockfd, MAX_NODES))
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in listening to %hu :: Reason %s\n", myID, ntohs(server.sin_port), strerror(errno));
        exit(1);
    }

    // set socket timeout
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 100000;
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    {
        printf("ERROR :: Node %d: createReceiveSocket -> Error in creating timeout for TCP connection :: Reason %s\n", myID, strerror(errno));
        exit(1);
    }
    return sockfd;
}

bool sendMessage(int myID, int dstID, messageType type)
{
    assert(myID != dstID);
    Message message;
    message.senderID = myID;
    message.type = type;

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
        printf("WARN :: Node %d: sendMessage -> Error in sending message-type %s to %d :: Reason - %s\n", myID, ((type == PRIVILEGE) ? "PRIVILEGE" : "REQUEST"), dstID, strerror(errno));
        close(sockfd);
        return false;
    }

    close(sockfd);
    return true;
}

/*
    Raymond Tree Algorithm
    For function descriptions of assignPrivilege , makeRequest
    refer https://turing.cs.hbg.psu.edu/comp512.papers/Raymond-89.pdf
*/
void assignPrivilege(int myID, int &holder, int &enterCS, int &asked, queue<int> &request_q, Time &start, FILE* fp, mutex *lock)
{
    lock->lock();

    if (holder == myID && enterCS == FALSE && request_q.size() != 0)
    {
        holder = request_q.front();
        request_q.pop();
        asked = FALSE;
        if (holder == myID)
        {
            // granting CS access to self
            long long int sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
            fprintf(fp, "%d assigns PRIVILEGE to itself at %lld\n", myID, sysTime);
            fflush(fp);
            enterCS = TRUE;
        }
        else
        {
            // send privilege to other node
            long long int sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
            fprintf(fp, "%d assigns PRIVILEGE to %d at %lld\n", myID, holder, sysTime);
            fflush(fp);
            if(sendMessage(myID, holder, PRIVILEGE) == false) {
                printf("ERROR :: Node %d: assignPrivilege -> Unable to send PRIVILEGE to %d\n", myID, holder);
                exit(1);
            }
        }
    }
    lock->unlock();
}

void makeRequest(int myID, int &holder, int &asked, queue<int> &request_q, Time &start, FILE* fp, mutex *lock) {
    lock->lock();

    if (holder != myID && request_q.size() != 0 && asked == FALSE) {
        asked = TRUE;
        long long int sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d sends REQUEST to Holder=%d for %d at %lld\n", myID, holder, request_q.front(), sysTime);
        fflush(fp);
        // request for privilege from holder
        if(sendMessage(myID, holder, REQUEST) == false) {
            printf("ERROR :: Node %d: makeRequest -> Unable to send REQUEST to %d\n", myID, holder);
            exit(1);
        }
    }

    lock->unlock();
}

void requestCS(int myID, int &holder, int &enterCS, int &asked, queue<int> &request_q, Time &start, FILE* fp, mutex *lock)
{
    lock->lock();
    // add my own request
    request_q.push(myID);
    lock->unlock();

    assignPrivilege(myID, holder, enterCS, asked, request_q, start, fp, lock);
    makeRequest(myID, holder, asked, request_q, start, fp, lock);

    while(true) {
        lock->lock();
        if(enterCS == TRUE) {
            lock->unlock();
            break;
        }
        lock->unlock();
        // std::this_thread::sleep_for(chrono::milliseconds(10));
    }
}

void exitCS(int myID, int &holder, int &enterCS, int &asked, queue<int> &request_q, Time &start, FILE* fp, mutex *lock)
{
    lock->lock();
    enterCS = FALSE;
    lock->unlock();
    assignPrivilege(myID, holder, enterCS, asked, request_q, start, fp, lock);
    makeRequest(myID, holder, asked, request_q, start, fp, lock);
}

void working(int myID, int &holder, int &enterCS, int &asked, queue<int> &request_q, atomic<int> &finishedProcessesCount,
            int noOfNodes, int mutualExclusionCounts, float alpha, float beta, Time &start, FILE *fp, mutex *lock)
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
        requestCS(myID, holder, enterCS, asked, request_q, start, fp, lock);
        totalResponseTime += chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - requestCSTime).count();

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d ENTERS CS for the %dst time at %lld\n", myID, i, sysTime);
        fflush(fp);
        sleep(inCSTime);

        sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
        fprintf(fp, "%d EXITS CS for the %dst time at %lld\n", myID, i, sysTime);
        fflush(fp);
        exitCS(myID, holder, enterCS, asked, request_q, start, fp, lock);
    }
    sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
    fprintf(fp, "%d completed all %d transactions at %lld\n", myID, mutualExclusionCounts, sysTime);
    fflush(fp);

    // node is alive as long as all processes have finished
    // other nodes might need routing of privilege across completed nodes

    // informing all other nodes of termination
    for(int i = 0; i < noOfNodes; i++)
    {
        if(i != myID) 
        {
            sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
            fprintf(fp, "%d sends TERMINATE to %d at %lld\n", myID, i, sysTime);
            fflush(fp);
            if (sendMessage(myID, i, TERMINATE) == false)
            {
                printf("ERROR :: Node %d: working -> Could not send TERMINATE to %d\n", myID, i);
                exit(EXIT_FAILURE);
            }
        }
    }

    finishedProcessesCount++;
    while(finishedProcessesCount < noOfNodes) {
        assignPrivilege(myID, holder, enterCS, asked, request_q, start, fp, lock);
        makeRequest(myID, holder, asked, request_q, start, fp, lock);
        // std::this_thread::sleep_for(chrono::milliseconds(10));
    }
    sysTime = chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now() - start).count();
    fprintf(fp, "%d finished any pending transactions at %lld\n", myID, sysTime);
    fflush(fp);
}

void receiveMessage(int myID, int myPort, int noOfNodes, int &holder, int &enterCS, int &asked, queue<int> &request_q,
                     atomic<int> &finishedProcessesCount, Time &start, FILE *fp, mutex *lock)
{
    struct sockaddr_in client;
    socklen_t len = sizeof(struct sockaddr_in);

    Message message;
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
                    fprintf(fp, "%d received PRIVILEGE from %d at %lld\n", myID, message.senderID, sysTime);
                    fflush(fp);
                    lock->lock();
                    holder = myID;
                    lock->unlock();
                    assignPrivilege(myID, holder, enterCS, asked, request_q, start, fp, lock);
                    makeRequest(myID, holder, asked, request_q, start, fp, lock);
                    break;
                case REQUEST:
                    sysTime = chrono::duration_cast<chrono::microseconds>(now - start).count();
                    fprintf(fp, "%d received REQUEST from %d at %lld\n", myID, message.senderID, sysTime);
                    fflush(fp);
                    lock->lock();
                    request_q.push(message.senderID);
                    lock->unlock();
                    assignPrivilege(myID, holder, enterCS, asked, request_q, start, fp, lock);
                    makeRequest(myID, holder, asked, request_q, start, fp, lock);
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

    // since graph is completely connected the spanning tree is a tree with just 1 level and rooted at initialTokenNode
    vector<int> holder(noOfNodes, initialTokenNode);
    vector<int> enterCS(noOfNodes, FALSE);
    vector<int> asked(noOfNodes, FALSE);
    vector<queue<int>> request_q(noOfNodes, queue<int>());
    vector<atomic<int> > finishedProcessesCount(noOfNodes);

    vector<mutex> locks(noOfNodes);

    printf("Creating receiver threads\n");

    // starting the receiver threads
    for (int i = 0; i < noOfNodes; i++)
    {
        finishedProcessesCount[i] = ATOMIC_VAR_INIT(0);

        workerReceivers[i] = thread(receiveMessage, i, startPort + i, noOfNodes,
            ref(holder[i]), ref(enterCS[i]), ref(asked[i]), ref(request_q[i]), 
            ref(finishedProcessesCount[i]), ref(start), fp, &locks[i]);
    }
    std::this_thread::sleep_for(chrono::seconds(1));

    printf("Creating CS executor threads\n");

    // starting the sender threads
    for (int i = 0; i < noOfNodes; i++)
    {
        workerSenders[i] = thread(working, i, 
            ref(holder[i]), ref(enterCS[i]), ref(asked[i]), ref(request_q[i]), ref(finishedProcessesCount[i]),
            noOfNodes, mutualExclusionCounts, alpha, beta, ref(start), fp, &locks[i]);
    }

    for (int i = 0; i < noOfNodes; i++)
    {
        workerSenders[i].join();
        workerReceivers[i].join();
    }
    float averageMessagesExchanged = totalReceivedMessages / (noOfNodes*1.0);
    float averageResponseTime = totalResponseTime / (1000.0 * noOfNodes * mutualExclusionCounts);
    cout << "\n\nAnalysis:\n\tTotal Messages Exchanged:  " << totalReceivedMessages << "\n\tAverage Messages Exchanged: " << averageMessagesExchanged << endl;
    cout <<"\n\tAverage Response Time: " << averageResponseTime << " milliseconds" << endl;
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