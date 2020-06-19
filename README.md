# **Synopsis Data Engine (SDE)**

## A stateful SDE with the basic impelentation of:

-  CountMin
-  BlommFilter
-  AMS sketch
-  TimeSeries (DFT)
-  LSH
-  Dynamic Add Sketch

## Input MAIN CLASS (Run)
~~~
args[0]={@link #kafkaDataInputTopic} DEFAULT: "Forex")
args[1]={@link #kafkaRequestInputTopic} DEFAULT: "Requests")
args[2]={@link #kafkaOutputTopic} (DEFAULT: "OUT")
args[3]={@link #kafkaBrokersList} (DEFAULT: "localhost:9092")
args[4]={@link #parallelism} Job parallelism (DEFAULT: "4")
~~~

SDE reads the Data and the Requests from a Kafka Topic each, given as arguments.
Based on the Request's KEY - VALUE pair SDE performs different actions.



| KEY - Stream ID | VALUE - Sketch | VALUE - SynopsisID | VALUE - Parameters |
| --- | ------- | ---------|-----|
| Stream ID| CountMin |1 | epsilon, confidence, seed
| Stream ID| BloomFilter |2 | numberOfElements, FalsePositive
| Stream ID| AMS Sketch |3 | Depth, Buckets
| Stream ID| DFT |4|  Basic Window Size
| Stream ID| LSH |5 | unfinished
| Stream ID| Dynamic Add Sketch | 6  | JarPath, ClassPath


---

## Getting Started

- git clone https://ATHENAINFORE@bitbucket.org/infore_research_project/6.1-sde.git
- mvn package
- You can find the  "Fat-Jar" under the Directory ./target with all the dependencies included

### Contact

For any questions don't hesitate to ask.!!!





