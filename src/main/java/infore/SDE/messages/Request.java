package infore.SDE.messages;



import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Arrays;

public class Request implements Serializable{
	
	private static final long serialVersionUID = 1L;
	private String DataSetkey; //hash value
	private int RequestID; //request type
	private int SynopsisID; // synopsis type
	private int UID; // unique identifier for each Request
	private String StreamID; //the stream ID
	private String[] Param; // the parameters of the Request
	private int NoOfP; // Number of parallelism 


	public Request(){

	}

	public Request(String key, int requestID, int synopsisID, int uID, String streamID, String[] param, int noOfP) {
		this.DataSetkey = key;
		RequestID = requestID;
		SynopsisID = synopsisID;
		UID = uID;
		StreamID = streamID;
		Param = param;
		NoOfP = noOfP;
	}

	public Request(String replace, String[] valueTokens) {
		this.DataSetkey = replace;
		RequestID = Integer.parseInt(valueTokens[1]);
		SynopsisID = Integer.parseInt(valueTokens[3]);
		UID = Integer.parseInt(valueTokens[2]);
		StreamID = valueTokens[4];
		Param = valueTokens[5].split(";");
		NoOfP = Integer.parseInt(valueTokens[6]);
	}

	public Request(String[] valueTokens) {
		this.DataSetkey = valueTokens[0];
		RequestID = Integer.parseInt(valueTokens[1]);
		SynopsisID = Integer.parseInt(valueTokens[3]);
		UID = Integer.parseInt(valueTokens[2]);
		StreamID = valueTokens[4];
		Param = valueTokens[5].split(";");
		NoOfP = Integer.parseInt(valueTokens[6]);
	}

	public String getDataSetkey() {
		return DataSetkey;
	}

	public void setDataSetkey(String key) {
		this.DataSetkey = key;
	}

	public String getKey() {
		return getDataSetkey();
	}
	public void setKey(String key) {
		this.DataSetkey = key;
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

	public int getUID() {
		return UID;
	}

	public void setUID(int uID) {
		UID = uID;
	}

	public String getStreamID() {
		return StreamID;
	}

	public void setStreamID(String streamID) {
		StreamID = streamID;
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

	@Override
	public String toString() {
		return "Request [key=" + DataSetkey + ", RequestID=" + RequestID + ", SynopsisID=" + SynopsisID + ", UID=" + UID
				+ ", StreamID=" + StreamID + ", Param=" + Arrays.toString(Param) + ", NoOfP=" + NoOfP + "]";
	}
	public String toSumString() {
		return  "[" + UID + "," + SynopsisID + "," + Arrays.toString(Param)  + "," + NoOfP + "]\n";
	}

	public String toJsonString() throws JsonProcessingException {
		return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
	}

	public String toKafkaProducer() {
		String pr ="";
		for(int i=0; i< Param.length; i++) {

			if(i == Param.length -1)
				pr = pr + Param[i];
			else
			pr = pr + Param[i]+";";
		}
		
		
		return "\"" +DataSetkey+","+RequestID+","+UID+","+SynopsisID+","+StreamID+","+pr+","+NoOfP+"\"";

	}
}
