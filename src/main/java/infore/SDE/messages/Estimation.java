package infore.SDE.messages;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Arrays;

public class Estimation implements Serializable {

	private static final long serialVersionUID = 1L;
	private String key; //hash value
	private String estimationkey; //the key of the Estimation
	private String StreamID; //the key of the Stream
	private int UID; // unique identifier for each Request
	private int RequestID; // request type
	private int SynopsisID; // Synopsis type
	private Object estimation; // the value of the Estimation
	private String[] Param; // the parameters of the Request
	private int NoOfP; // number of parallelism
	
	public Estimation(int uID, String estimationkey, int requestID, int synopsisID, String key, Object estimation, String[] param,
			int noOfP) {
		this.UID = uID;
		this.estimationkey = estimationkey;
		this.RequestID = requestID;
		this.SynopsisID = synopsisID;
		this.key = key;
		this.estimation = estimation;
		this.Param = param;
		this.NoOfP = noOfP;
	}
	
	public Estimation(Request rq, Object estimate,  String estimationkey) {

		// TODO Auto-generated constructor stub
		this.key = estimationkey;
		this.estimationkey = estimationkey;
		this.estimation = estimate;
		this.RequestID = rq.getRequestID();
		this.SynopsisID = rq.getSynopsisID();
		this.StreamID = rq.getStreamID();
		this.Param = rq.getParam();
		this.NoOfP = rq.getNoOfP();
		this.UID = rq.getUID();	

	}

	public Estimation(String[] valueTokens) {

		this.key = valueTokens[0];
		this.estimationkey = valueTokens[1];
		this.StreamID = valueTokens[2];
		this.UID = Integer.parseInt(valueTokens[3]);	
		this.RequestID = Integer.parseInt(valueTokens[4]);
		this.SynopsisID =Integer.parseInt(valueTokens[5]);
		this.estimation = valueTokens[6];
		this.Param = valueTokens[7].split(";");
		this.NoOfP = Integer.parseInt(valueTokens[8]);
		
	}

    public Estimation(Estimation e) {
		this.key = e.getEstimationkey();
		this.estimationkey = e.getEstimationkey();
		this.StreamID = e.getStreamID();
		this.UID = e.getUID();
		this.RequestID = e.getRequestID();
		this.SynopsisID =e.getSynopsisID();
		this.estimation = e.getEstimation();
		this.Param = e.getParam();
		this.NoOfP = e.getNoOfP();



    }

    public String getEstimationkey() {
		return estimationkey;
	}
	public String getStreamID() {
		return StreamID;
	}

	public void setStreamID(String streamID) {
		StreamID = streamID;
	}
	public void setEstimationkey(String estimationkey) {
		this.estimationkey = estimationkey;
	}

	public int getRequestID() {
		return RequestID;
	}

	public void setRequestID(int requestID) {
		RequestID = requestID;
	}

	public int getSynopsisID() {
		return SynopsisID;
	}

	public void setSynopsisID(int synopsisID) {
		SynopsisID = synopsisID;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public Object getEstimation() {
		return estimation;
	}

	public void setEstimation(Object estimation) {
		this.estimation = estimation;
	}

	public String[] getParam() {
		return Param;
	}

	public void setParam(String[] param) {
		Param = param;
	}

	public int getNoOfP() {
		return NoOfP;
	}

	public void setNoOfP(int noOfP) {
		NoOfP = noOfP;
	}

	public int getUID() {
		return UID;
	}

	public void setUID(int uID) {
		UID = uID;
	}

	@Override
	public String toString() {
		return "Estimation [key=" + key + ", estimationkey=" + estimationkey + ", StreamID=" + StreamID + ", UID=" + UID
				+ ", RequestID=" + RequestID + ", SynopsisID=" + SynopsisID + ", Param=" + Arrays.toString(Param)
				+ ", NoOfP=" + NoOfP + "]";
	}

	public String toJsonString() throws JsonProcessingException {
		return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
	}

	public byte[] toKafka() {
		
        String par = Arrays.toString(Param).replace(",", ";");
        par = par.substring(1, par.length()-1).replaceAll("\\s+","");
		return ("\""+estimationkey+","+StreamID+","+UID+","+RequestID+","+SynopsisID+","+estimation+","+par+","+NoOfP+"\"").getBytes();
		//return ("\"KEY:_"+estimationkey+" SYNOPSIS:_"+SynopsisID+" ESTIMATION:_"+estimation+"_\"").getBytes();
	}
	public byte[] toKafkaJson() throws JsonProcessingException {

		 return toJsonString().getBytes();
	}

}
