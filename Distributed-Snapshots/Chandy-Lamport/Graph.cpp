#include <map>
#include <queue>
#include <vector>
#include <climits>
#include <iostream>
#include <algorithm>
#include <arpa/inet.h>

using namespace std;

int startPort = 10000;
#define MAX_NODES 30

class Graph
{
  public:
    int V;
    vector<vector<int>> adj;
    vector<vector<uint16_t>> routingTable;
    vector<vector<int>> neighbors;  //coordinator not there in neighbors
    vector<uint16_t> portMapping;
    map<uint16_t, int> portNodeMapping;

    Graph(int vertices);
    void AddEdge(int u, int v);

    vector<int> Dijsktra(int v);
    void ConstructRoutingTable();
    void PrintRoutingTable();
    int unionFind(vector<int> &vec, int root, int curr);

    ~Graph();
};

Graph::Graph(int vertices)
{
    V = vertices;
    adj = vector<vector<int>>(V);
    routingTable = vector<vector<uint16_t>>(V);
    neighbors = vector<vector<int>>(V);
    portMapping = vector<uint16_t>(V);

    for (int i = 0; i < V; i++)
    {
        portMapping[i] = htons(startPort + i);
        portNodeMapping[htons(startPort + i)] = i;
    }
}

Graph::~Graph()
{
    cout << "Cleaning Graph object" << endl;
    portMapping.clear();
    for (int i = 0; i < V; i++)
    {
        adj[i].clear();
        routingTable[i].clear();
        neighbors[i].clear();
    }
    adj.clear();
    routingTable.clear();
    neighbors.clear();
}

typedef struct HeapNode
{
    int dis;
    int index;
} HeapNode;

struct MinComparator
{
    bool operator()(const HeapNode &a, const HeapNode &b)
    {
        return a.dis > b.dis;
    }
};

void Graph::AddEdge(int u, int v)
{
    adj[u].push_back(v);
}

vector<int> Graph::Dijsktra(int v)
{
    vector<int> parent(this->V, -1);
    vector<int> dist(this->V, INT_MAX);
    dist[v] = 0;
    parent[v] = -1;

    priority_queue<HeapNode, vector<HeapNode>, MinComparator> pq;
    pq.push(HeapNode{0, v});

    while (!pq.empty())
    {
        auto n = pq.top();
        pq.pop();

        for (auto &nbr : adj[n.index])
        {
            if (dist[nbr] > dist[n.index] + 1)
            {
                dist[nbr] = dist[n.index] + 1;
                parent[nbr] = n.index;
                pq.push(HeapNode{dist[nbr], nbr});
            }
        }
    }
    cout << "Root :: " << v << "\t\t-->\t\t";

    for (int i = 0; i < V; i++)
    {
        cout << "(" << i << "::" << parent[i] << ") , ";
    }
    cout << endl;
    return parent;
}

int Graph::unionFind(vector<int> &vec, int root, int curr)
{
    if (vec[curr] == -1)
    {
        return curr;
    }
    if (vec[curr] == root || vec[curr] == curr)
    {
        return curr;
    }
    return unionFind(vec, root, vec[curr]);
}

void Graph::ConstructRoutingTable()
{
    cout << "\n\n -- Parents Node to Root using Dijsktra (NodeID, ParentNodeId) -- \n\n";

    for (int root = 0; root < V; root++)
    {
        vector<int> parent = Dijsktra(root);
        vector<uint16_t> parentPort(V);

        for (int j = 0; j < V; j++)
        {
            parent[j] = unionFind(parent, root, j);
        }

        for (int j = 0; j < V; j++)
        {
            parentPort[j] = portMapping[parent[j]];
        }
        for (auto &nbr : adj[root])
        {
            if (nbr != 0)
            {
                neighbors[root].push_back(nbr);
            }
        }
        routingTable[root] = parentPort;
    }
}

void Graph::PrintRoutingTable()
{
    cout << "\n\n -- Port Mapping (NodeID, PortNumber) -- \n\n";
    for (int i = 0; i < V; i++)
    {
        cout << "Process #" << i << " : " << ntohs(portMapping[i]) << endl;
    }

    cout << "\n\n -- Adjacency (NodeID, NeighborId) -- \n\n";
    for (int root = 0; root < V; root++)
    {
        cout << "Root :: " << root << "\t\t-->\t\t";
        for (int nbr : adj[root])
        {
            cout << nbr << " , ";
        }
        cout << endl;
    }

    cout << "\n\n -- Neighbors (NodeID, NeighborId) -- \n\n";
    for (int root = 0; root < V; root++)
    {
        cout << "Root :: " << root << "\t\t-->\t\t";
        for (int nbr : neighbors[root])
        {
            cout << nbr << " , ";
        }
        cout << endl;
    }

    cout << "\n\n -- Routing Table (NodeID, NeighborId) -- \n\n";
    for (int root = 0; root < V; root++)
    {
        cout << "Root :: " << root << "\t\t-->\t\t";
        for (int j = 0; j < V; j++)
        {
            cout << "(" << j << "::" << ntohs(routingTable[root][j]) << ") , ";
        }
        cout << endl;
    }
    cout << "\n\n\n\n";
}