# **A Client for the Synopsis Data Engine (CSDE)**

This project provides **3 Java main classes**:
---
### how to send data (SendData)

```
 create a new DataPoint
 setKey(String DataSetkey)	
 setData(String Data, String KeyToUseIndex) 
 create a kafka Producer
 sendData((kafka Configuration),DataToKafkaSerialization(record))
```

---

### how to send requests for adding Synopses, Delete, Estimate
```
  main(kafka Configuration)
  create a new Request
  SetRequestID(int RequestID)
  setKey(String DataSetkey)
  setUID(int UID)
  setSynopsis(int Synopsis)
  setParameters(String DataSetkey)
  setParallelism(int i)
  create a kafka Producer
  sendRequest((kafka Configuration),DataToKafkaSerialization(record))
```

---
### how to getState

```

 FlinkQuerableState
(SunopsisID, UID, Parameters, Parallelism) GetRunningSynopses(DataSetKey); 

```

## a small Demo

#### Step 1: Run the SDE

-> flink run SDE.jar  "6FIN500" "testRequest5" "OUT" "localhost:9092" "4"  

---

#### Step 2: Start a new Synopsis

-> Run SendRequest MainClass 

Request Add synopsis
```
{
  "streamID" : "ForexALLNoExpiry",
  "synopsisID" : 4,
  "requestID" : 1,
  "dataSetkey" : "Financial_Data",
  "param" : [ "StockID", "price","Queryable", “0.0002“, “0.99", “4" ],
  "NumberOfParallelism" : 4,
  "uid" : 1110
}
```
CountMin Parameters

  + KeyField = the field to use as a key | example:StockID
  + ValueField = the field to use as a value| example:price
  + OperationMode = Queryable
  + epsilon = 0.0002
  + cofidence = 0.99
  + seed = 4

---

#### Step 3: Send Data

-> Run SendData MainClass

The data format is in JSON, the main fields DataSetKey,streamID, values

```
{
  "values" : 
	  "{"time":"02/19/2019 06:07:14",
	  "StockID":"ForexALLNoExpiry",
	  "price":"110.11"}",
  "streamID" : "7040",
  "dataSetkey" : "FINANCIAL_USECASE"
}
```
---

#### Step 4: Send an Estimation Request

-> Run Send Estimation Request Main Class

Estimation Request

```
{
  "streamID" : "ForexALLNoExpiry",
  "synopsisID" : 4,
  "requestID" : 3,
  "dataSetkey" : "Financial_Data",
  "param" : [ "ForexALLNoExpiry" ],
  "NumberOfParallelism" : 4,
  "uid" : 1110
}
```

---




### Contact

For any questions don't hesitate to ask.!!!