#include <mutex>
#include <vector>
#include <thread>
#include <random>
#include <chrono>
#include <time.h>
#include <future>
#include <climits>
#include <fstream>
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>

#include <arpa/inet.h>

using namespace std;
#define MAX_CLIENTS 30
#define MAX_DATA 1024

int startPort = 9000;

std::atomic<int> numberOfCompletedNodes = ATOMIC_VAR_INIT(0);

void split(string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while (getline(ss, item, delim)) {
        elems.push_back(item);
    }
}

vector<string> split(string &s, char delim) {
    vector<string> elems;
    split(s, delim, elems);
    return elems;
}

void recvEvent(int id, int sock_id, int port, int noOfNodes, chrono::time_point<chrono::system_clock> start, vector<int> &vectorClock, vector<int> &LU, FILE* fp, mutex* lock) {
    int client_id;
    struct sockaddr_in server , client;
    socklen_t len = sizeof(struct sockaddr_in), data_len;

    char data[MAX_DATA];

    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = INADDR_ANY;
    bzero(&server.sin_zero, 0);

    // binding TCP server to IP and port
    if( bind(sock_id, (struct sockaddr*) &server, len) < 0) {
        printf("Thread %d: caller : recvEvent -> Error in binding socket for %d\n", id, port);
        exit(-1);
    }

    // mark it for listening
    if( listen(sock_id, MAX_CLIENTS)) {
        printf("Thread %d: caller : recvEvent -> Error in listening for %d\n", id, port);
        exit(-1);
    }
    printf("Thread %d: caller : recvEvent -> Started Listening for %d\n", id, port);

	struct timeval tv;
	tv.tv_sec = 0;
	tv.tv_usec = 100000;
	if (setsockopt(sock_id, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
		cout << "Error in creating timeout for TCP connection " << endl;
		return;
	}

    while(numberOfCompletedNodes != noOfNodes) {
        
        if((client_id = accept(sock_id, (struct sockaddr*) &client, &len)) < 0) {
        	continue;
        }

        // receing message from client
        int data_len = recv(client_id, data, MAX_DATA, 0);
        if(data_len > 0) {

			int counter = 0;
    		lock->lock();

		    vectorClock[id]++;
		    LU[id] = vectorClock[id]; 	//LU always saves the current state of vectorClock[id]

		    string str = string(data);
		    std::vector<string> val = split(str,' ');
			int pid = stoi(val[0]);
			int msg = stoi(val[1]);

			for (int i = 2; i < val.size(); i+=2) {
		    	int index = stoi(val[i]);
		    	int value = stoi(val[i+1]);

		    	//updating only the necessary values
		        if (vectorClock[index] < value) {
		        	vectorClock[index] = value;
		        	LU[index] = vectorClock[id];
		        }
			}

			string vclk = "[ ";
		    for(auto&& i : vectorClock) {
		    	vclk += to_string(i) + " ";
		    }
		    vclk += "]";
		    chrono::time_point<chrono::system_clock> now = chrono::system_clock::now();;
			long long int sysTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
			fprintf(fp, "Process#%d receives message m%d from Process#%d at %lld, vc: %s\n", id, msg, pid, sysTime , vclk.c_str()); fflush(fp);

		    lock->unlock();

	    	printf("\033[1;33mThread %d: caller : recvEvent -> Received successfully from %d \033[0m\n", id, pid);
        } else {
        	printf("Thread %d: caller : recvEvent -> Error in receiving data\n", id);
        }
        close(client_id);
    }
    close(sock_id);
}

int sendEvent(int id, int sendToId, chrono::time_point<chrono::system_clock> start, vector<int> &vectorClock, vector<int> &LS, vector<int> &LU, FILE* fp, mutex* lock) {
    struct sockaddr_in server;
    int sock_id;
    char input[MAX_DATA];
    int port = startPort + sendToId;

    // creating the socket
    if((sock_id = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("Thread %d: caller : sendEvent -> Error in creating socket for %d\n", id, port);
        exit(-1);
    }
    server.sin_family = AF_INET;
    server.sin_port = htons(port);
    server.sin_addr.s_addr = inet_addr("127.0.0.1");
    bzero(&server.sin_zero, 8);

    // connecting TCP to server
    if(connect(sock_id, (struct sockaddr*) &server, sizeof(struct sockaddr_in) ) < 0) {
        printf("Thread %d: caller : sendEvent -> Error in connecting to server for %d\n", id, port);
        exit(-1);
    }
	char buffer[20];

	//adding sender id and random number in the message
	strcpy(input, to_string(id).c_str());
	strcat(input, " ");

	int msg = 10 + rand() % 90;
	strcat(input, to_string(msg).c_str());
	strcat(input, " ");

	lock->lock();

	vectorClock[id]++;
	LU[id] = vectorClock[id];

	int n = vectorClock.size();
	int noOfMessageEntries = 0;

    for (int i = 0; i < n; ++i) {

    	if (LS[sendToId] < LU[i]) {

    		noOfMessageEntries+=2;

    		//adding index and vectorclock value to message
	    	strcpy(buffer, to_string(i).c_str());
	    	strcat(input, buffer);

	    	strcat(input, " ");

	    	strcpy(buffer, to_string(vectorClock[i]).c_str());
	    	strcat(input, buffer);

	    	strcat(input, " ");	    		
    	}
    }

    LS[sendToId] = vectorClock[id];

	chrono::time_point<chrono::system_clock> now = chrono::system_clock::now();
	long long int sysTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
	string vclk = "[ ";
    for(auto&& i : vectorClock) {
    	vclk += to_string(i) + " ";
    }
    vclk += "]";
	fprintf(fp, "Process#%d sends (%d vector entries) message m%d to Process#%d at %lld, vc: %s\n", id, noOfMessageEntries, msg, sendToId, sysTime, vclk.c_str()); fflush(fp);

	lock->unlock();

    if (send(sock_id, input, MAX_DATA, 0) < 0) {
    	printf("Thread %d: caller : sendEvent -> Error in sending message to %d\n", id, port);
    } else {
    	printf("\033[1;32mThread %d: caller : sendEvent -> Sent Vector Successfully to %d\033[0m\n", id, port);
    }
    close(sock_id);
    return noOfMessageEntries;
}

void internalEvent(int id, int eventNumber, chrono::time_point<chrono::system_clock> start, vector<int> &vectorClock, vector<int> &LU, FILE* fp, mutex *lock) {
	lock->lock();

	vectorClock[id]++;
	LU[id] = vectorClock[id];

	chrono::time_point<chrono::system_clock> now = chrono::system_clock::now();
	long long int sysTime = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
	string vclk = "[ ";
    for(auto&& i : vectorClock) {
    	vclk += to_string(i) + " ";
    }
    vclk += "]";
	fprintf(fp, "Process#%d executes internal event e%d at %lld, vc: %s\n", id, eventNumber, sysTime , vclk.c_str()); fflush(fp);

	lock->unlock();

	printf("\033[1;34mThread %d: caller : interalEvent #%d \033[0m\n", id, eventNumber);
}

int run(int id, int noOfNodes, float lambda, int noOfInternalEvents, int noOfMessageEvents, vector<int> &neighbors) {

	vector<int> vectorClock(noOfNodes, 0);
	vector<int> LS(noOfNodes, 0);
	vector<int> LU(noOfNodes, 0);

    // creating the recv socket
    int recv_sock_id;
    if((recv_sock_id = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        printf("Thread #%d: caller : recvEvent -> Error in making the socket for %d\n", id, startPort + id);
        exit(-1);
    }

	mutex* lock = new mutex;
	FILE* fp = fopen ("output.txt", "a+");

	chrono::time_point<chrono::system_clock> start = chrono::system_clock::now();
	thread recv = thread(recvEvent, id, recv_sock_id, startPort + id, noOfNodes, start, ref(vectorClock), ref(LU), fp, lock);
	// auto recvHandle = recv.native_handle();
	
	std::this_thread::sleep_for(std::chrono::milliseconds(1000)); 	// wait for all recv ports to be up

	random_device random_device;
	mt19937 engine{random_device()};
	uniform_int_distribution<int> dist(0, neighbors.size()-1);

	default_random_engine generator;
	exponential_distribution<double> distribution(1 / lambda);

	int totalNoOfInternalEvents = noOfInternalEvents + 1;
	int totalNoOfMessageEvents = noOfMessageEvents;
	int totalNumberOfEntriesSent = 0;

	while(true) {

		if (noOfInternalEvents > 0 && noOfMessageEvents > 0) {
			if(rand()%2 == 0) {
				internalEvent(id, totalNoOfInternalEvents - noOfInternalEvents, start, vectorClock, LU, fp, lock);
				noOfInternalEvents--;
			} else {
				totalNumberOfEntriesSent += sendEvent(id, neighbors[dist(engine)], start, vectorClock, LS, LU, fp, lock);
				noOfMessageEvents--;
			}
			sleep(distribution(generator));
		} else {
			for (int i = noOfMessageEvents; i > 0; --i) {
				totalNumberOfEntriesSent += sendEvent(id, neighbors[dist(engine)], start, vectorClock, LS, LU, fp, lock);
				sleep(distribution(generator));				
			}
			for (int i = noOfInternalEvents; i > 0; --i) {
				internalEvent(id, totalNoOfInternalEvents - i, start, vectorClock, LU, fp, lock);
				sleep(distribution(generator));				
			}
			break;
		}
	}
    printf("Thread %d: All send & internal events completed.\n", id);
    printf("Thread %d: Waiting for remaining threads to finish.\n", id);

    numberOfCompletedNodes++;
    while(numberOfCompletedNodes != noOfNodes);

    printf("Thread %d: All threads have finished.\n", id);

    recv.join();

    printf("\n\nThread %d:\n\t%d integers(%lu bytes) in the vector clock.\n\t%f average number of vector entries(%f bytes) sent in send message.\n\n", id, noOfNodes, noOfNodes*sizeof(int), totalNumberOfEntriesSent*1.0/totalNoOfMessageEvents, (totalNumberOfEntriesSent*1.0/totalNoOfMessageEvents)*sizeof(int));

    delete lock;
	fclose(fp);
	return totalNumberOfEntriesSent;
}

int main (int argc, char *argv[]) {

	ifstream fin("inp-params.txt");
	
	if(argc == 1) {
		startPort = 10000;
	} else {
		startPort = atoi(argv[1]);
	}

	if (!fin) {
		cout << "\033[1;31mError In Opening inp-params.txt\033[0m\n";
		exit(EXIT_FAILURE);
	}

	srand (time(NULL));
	fclose(fopen("output.txt", "w"));

	//reading necessary details from the FILE
	int noOfNodes;
	float lambda;
	float alpha;
	int noOfMessageEvents;

	fin >> noOfNodes;
	fin >> lambda;
	fin >> alpha;
	fin >> noOfMessageEvents;

	int noOfInternalEvents = alpha * noOfMessageEvents;

	string list;
	vector<vector<int> > topo(noOfNodes, vector<int>(0));
	while(!fin.eof()) {
		getline(fin, list);
		if (list.size() > 0) {
			istringstream ss(list);
	        string word;
	        ss >> word;
	        // convert 1-indexing to 0-indexing
	        int nodeId = stoi(word) - 1;
			while(true) {
				ss >> word;
				if(!ss) {
					break;
				}
	 			topo[nodeId].push_back(stoi(word) - 1); 
		    }
		}
	}

	fin.close();

	vector<future<int> > nodes;

	for (int i = 0; i < noOfNodes; i++) {
		nodes.push_back(async(run, i, noOfNodes, lambda, noOfInternalEvents, noOfMessageEvents, ref(topo[i])));
	}

	int totalNumberOfEntriesSent = 0;
	for (auto& thread : nodes) {
		totalNumberOfEntriesSent += thread.get();
	}

	float avgNumberOfEntriesSent = (totalNumberOfEntriesSent * 1.0) / (noOfNodes * noOfMessageEvents);

	cout << "\n\n\nFor #nodes - " << noOfNodes << ", using Singhal-Kshemkalyani optimization on average " << avgNumberOfEntriesSent << " vector entries (" << avgNumberOfEntriesSent*sizeof(int) << " bytes) were sent per message." << endl;

	return 0;
}