package message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Arrays;

public class Request implements Serializable {

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

    /**
     * @param dataSetkey
     * @param requestID
     * @param synopsisID
     * @param uID
     * @param streamID
     * @param param
     * @param noOfP
     */
    public Request(String dataSetkey, int requestID, int synopsisID, int uID, String streamID, String[] param, int noOfP) {
        this.DataSetkey = dataSetkey;
        RequestID = requestID;
        SynopsisID = synopsisID;
        UID = uID;
        StreamID = streamID;
        Param = param;
        NoOfP = noOfP;
    }


    public String getDataSetkey() {
        return DataSetkey;
    }

    public void setDataSetkey(String key) {
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
        return "Request [DataSetkey=" + DataSetkey + ", RequestID=" + RequestID + ", SynopsisID=" + SynopsisID + ", UID=" + UID
                + ", StreamID=" + StreamID + ", Param=" + Arrays.toString(Param) + ", NoOfP=" + NoOfP + "]";
    }
    public String ValueToKafka() {
        String pr ="";
        for(int i=0; i< Param.length; i++) {

            if(i == Param.length -1)
                pr = pr + Param[i];
            else
                pr = pr + Param[i]+";";
        }

        return "\"" +DataSetkey+","+RequestID+","+UID+","+SynopsisID+","+StreamID+","+pr+","+NoOfP+"\"";
    }

    public String toJsonString() throws JsonProcessingException {
        return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(this);
    }


    public String keyToKafka() {
        return "\"" +DataSetkey+"\"";
    }
}
