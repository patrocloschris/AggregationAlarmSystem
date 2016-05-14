# Design and Development of a Real-Time Aggregation and Alarm Generation Engine.

We are proposing to develop a distributed system which is expected to handle real-time (streaming) data coming from the network. Our system should be able to 
undertake various aggregation queries on the incoming datasets and produce alarms based on several configurable threshold rules. The incoming data will mainly refer to 
router statistics and may entail the following: 

* CPU
* RAM
* Active Sessions
* Uptime
* ID
* Name
* Site
* Temperature


Firstly, our system will be able to read the necessary information through TCP sockets. The next step will be to forward them into our distributed processing engine. At the time when 
an event is read from the engine, it will be distributed to various components capable of carrying out specific functionality. This functionality should be configurable through a dedicated database. 
After the end of processing, all the output information will be stored into a database as well.

In terms of implementation, we are willing to use the following frameworks: 

* *Apache* *Storm* (streaming and processing engine)
* *Cassandra* (column-oriented data store)

Initially, we had to choose the most appropriate processing engine to handle streaming data. The three predominant candidates were $Storm$, $Spark$ $Streaming$ and *Flink*. After researching 
various benchmarks online, we found out that $Storm$ and $Spark$ beat *Flink* in terms of performance. In retrospec, we ended up with choosing $Storm$, because it mainly allows dealing with streaming 
data in a more flexible manner via its Directed Acyclic Graph topology. Furthermore, it offers a convenient way of getting/reading data (through the network?). Finally, *Storm* seems more appropriate because 
its philosophy is based on task-parallel computations compared to *Spark's* data-parallel computations.

In similar spirit, we had to choose a suitable distributed data store. Column storage is advantageous for our application, because we need to mainly access a subset of columns with 
each request for aggregation purposes. After researching various studies online, *Cassandra* seems to be the best-performing column store among others (e.g. HBase).

To take the experimental results the data will we prepare than before. In this way we will arrange the rate that we send them to our system.
A good start can be to send 500 events/sec/node (500 events = ~ 2-4mb) and will scale with a step 250-500 events/sec/node each time.

The following metrics is our target it the end of the implementation

* How the process time of an event is influenced for several data volumes (previous scales), also to evaluate the overall system performance.
* The processing time if the events has a greater size than usual (ie on how the system is affected different throughtputs)
* How does the processing speed is affected if one or more nodes dropped.
* The CRUD operations speed if we dump one by one the events or if we dump them in batch mode in our DB.
* If a node does not have such a good performance (as HW) then that affected the rest of our Pipeline.
* What happens if the system drops the Master-Node (Nimbus)? Is single point of failure?
* What happens if we increase the number of nodes and how does this gradually increasing the overall Performance of the system. There is a point beyond which the increase of nodes leads performance degradation? The rate of events (workload) will send the system will remain stable during the process.