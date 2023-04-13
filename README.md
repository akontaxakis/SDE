# **And Synopses for All: a Synopses Data Engine for Extreme Scale Analytics-as-a-Service**

## SDEaaS: Synopses Data Engine-as-a-Service

## Cite as: work under review

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

### List of some Available Synopsis.

| SynopsisID | Synopsis| Estimation Parameters| Estimates| Mostly Used|	Parameters|
| ---------- | ------- | ---------------------|----------|------------|-----------|
|1|	CountMin |	KEY |	Count|	Frequent Itemsets|	KeyField, ValueField,OperationMode, epsilon, cofidence, seed |
|2|	BloomFilter| KEY |	Member of a Set| 	Membership|	KeyField, ValueField,OperationMode, numberOfElements, FalsePositive|
|3|	AMS|	KEY|	 L2 norm, innerProduct, Count|	Frequent Itemsets|	KeyField, ValueField,OperationMode, Depth, Buckets|
|4|	DFT|	similarity score|	Fourier Coefficients|	Correlation|	KeyField, ValueField,  timeField,OperationMode,Interval in Seconds, Basic Window Size in Seconds, Sliding Window Size in Seconds , #coefficients|
|5|	LSH	|none	|BucketID - Projected features	|Correlation|	KeyField, ValueField,OperationMode, windowSize, Dimensions, numberOfBuckets|
|6|	Coresets|	theNumberOfClustersK|	Coresets used for kmeans|	Clustering|	KeyField, ValueField,OperationMode, maxBucketSize,dimensions|
|7|	HyperLogLog|	none	|Cardinality	|Cardinality	|keyField, ValueField, OperationMode, rsd ( relative standard deviation )|
|8|	StickySampling	|KEY	|FrequentItems, isFrequent, Count|	Frequent Itemsets	|keyField, ValueField, OperationMode, support, epsilon, probabilityofFailure|
|9|	LossyCounting|	KEY|	Count, FrequentItems	|Frequent Itemsets	|keyField, ValueField, OperationMode, epsilon ( the maximum error bound )|
|10|	ChainSampler|	none|	Sample of the data|	Sampling|	keyField,  ValueField, OperationMode, size of sample, size of the window|
|11|	GKQuantiles|	KEY|	Quantile	|Quantiles|keyField , ValueField, OperationMode, epsilon ( the maximum error bound )|
|12|	MarinetimeSKetch|	none	|Ship positions(Sample)|	Sampling	|keyField, ValueField, OperationMode,  minsamplingperiod, minimumDistance, speed(knots) ,corse(degrees)| 
|13|	TopK|	none|	TopK|	TopK|	keyField, ValueField, OperationMode, numberOfK, countDown|
|14|	OptimalDistributedWindowSampling	|none	|Sample of the data	|Sampling|	keyField, ValueField, OperationMode, windowSize|
|15|	OptimalDistributedSampling|	none	|Sample of the data|	Sampling|	keyField, ValueField, OperationMode|
|16|	windowQuantiles|	KEY|	Quantile|	Quantiles	|keyField , ValueField, OperationMode, epsilon ( the maximum error bound ),windowSize|
17|	Radius Sketch Family| similarity score|	similarity score|	similarity/distance|	KeyField, ValueField, OperationMode,Group Size, Sketch Size,Window Size, Number of Groups|

---
### Full list of available Requests

|RequestID|	OperationType|Description|
| --------| ----------   |-----------|
|1|	ADD|add| Synopsis with Keyed partitioning| 
|2|	DELETE|delete a Synopsis|
|3|	ESTIMATE|request an estimation of a queryable Synopsis|
|4|	ADD|add Synopsis with Random partioning|
|5|	ADD|add continuous Synopsis|
|6|	ESTIMATE|request a more advance estimation|
|7|	UPDATE|	update a Synopsis state|


### Messages Example

Input Data Example
```sh
{
  "values" : 
	  "{"time":"02/19/2019 06:07:14",
	  "StockID":"ForexALLNoExpiry",
	  "price":"110.11"}",
  "streamID" : "ForexALLNoExpiry",
  "dataSetkey" : "Forex"
}

```
Request  Example for adding new Synopses
```sh
{
  "streamID" : "INTEL",
  "synopsisID" : 4,
  "requestID" : 1,
  "dataSetkey" : "Forex",
  "param" : [ "StockID", "price","Queryable", “1“, “720", “3600", "8" ],
  "noOfP" : 4,
  "uid" : 1110
}


```

## Getting Started

- git clone https://github.com/akontaxakis/SDE.git
- mvn package
- You can find the  "Fat-Jar" under the Directory ./target with all the dependencies included

### Contact

For any questions don't hesitate to ask.!!!
Antonios Kontaxakis, antonios.kontaxakis-ATNOSPAM-ulb.be
Nikos Giatrakos, ngiatrakos-ATNOSPAM-tuc.gr
Dimitris Sacharidis, dimitris.sacharidis-ATNOSPAM-ulb.be
Antonios Deligiannakis, adeligiannakis-ATNOSPAM-tuc.gr





