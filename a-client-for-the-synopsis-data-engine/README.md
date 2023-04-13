# **A Client for the Synopsis Data Engine (CSDE)**

## This project provides **3 Java main classes**:
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
### Send requests for adding Synopses, Delete, Estimate
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
###  getState
```
 FlinkQuerableState
(SunopsisID, UID, Parameters, Parallelism) GetRunningSynopses(DataSetKey); 
```
### a small DEMO

#### Step 1:

-> flink run SDE.jar  "6FIN500" "testRequest5" "OUT" "localhost:9092" "4"  

---

#### Step 2: Start a new Synopsis

-> Run SendRequest MainClass 

Request Add synopsis
```
KEY<DatasetKey> --  Value<DatasetKey,RequestID, UID, SynopsisID, StreamID, parameters, NumberOfParallelism>
"FINANCIAL_USECASE" -- "FINANCIAL_USECASE,1,111,1,INTEL,1;2;0.0002;0.99;4,1"
```

CountMin Parameters

  + KeyIndex = the field to use as a key | example:INTEL
 
  + ValueIndex = the field to use as a value| example:68
 
  + epsilon = example:0.0002
 
  + confidence = example: 0.99
 
  + seed = example:4
 
---

#### Step 3: Send Data

-> Run SendData MainClass

Data

```
KEY<DatasetKey,   Random>  --  Value<DataSetKey,     StreamID, Values>
"FINANCIAL_USECASE, 7040"  --   "FINANCIAL_USECASE,   INTEL,     68"
```

---

#### Step 4: Send an Estimation Request

-> Run SendEstimationRequest MainClass

Estimation

```
<estimationkey, StreamID, UID, RequestID,  SynopsisID, estimation, parameters, NumberOfParallelism>
<  111,          INTEL,   111,    3,           1,        38548,      INTEL,           1           >
```

---

### Contact

For any questions don't hesitate to ask.!!!