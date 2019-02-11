### Overview

This is a C++ implementation of <a href="../Distributed-Mutual-Exclusion-Algorithms(Ajay-Kshemkalyani-and-Mukesh-Singhal).pdf">Suzuki-Kasami’s Broadcast  Distributed Mutual Exclusion Algorithm</a>.

## How to Compile

```
g++ -std=c++14 -pthread Suzuki-Kasami.cpp -o suzuki
```

## How to Run

```
./suzuki ../inp-params.txt 9000

9000 denote the starting value of the range of port numbers where the port numbers for the different nodes will be allocated.
```
