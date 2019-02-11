### Overview

This is a C++ implementation of <a href="./Logical-Time(Ajay-Kshemkalyani-and-Mukesh-Singhal).pdff">Distributed Computing Vector Clocks</a> along with Singhal-Kshemkalyani-Optimization.

### How to Compile

```
g++ -pthread -o sk SK-cs15btech11019.cpp
g++ -pthread -o vc VC-cs15btech11019.cpp
```

### How to Run

```
./sk 9000
./vc 5000

The numbers denote the starting value of the range of port numbers where the port numbers for the different nodes will be allocated.
More details in the report.
```
