package lib.PastCOEF;

import org.apache.commons.math3.complex.Complex;

import java.util.Date;
import java.util.TreeMap;

public class pastCOEF {

    private TreeMap<Date, String> timeSeries;
    private TreeMap<Date, Complex[]> pastDFTs;//start of the current window->Date, DFTs->array of complex numbers
    private String bucketID;
    private String streamID;
    private String neighbors;

    public pastCOEF() {
        pastDFTs = new TreeMap<>();
        timeSeries = new TreeMap<>();
    }

    public void setNeighbors(String neighbors) { this.neighbors = neighbors; }
    public String getNeighbors() { return neighbors; }
    public void setBucketID(String bucketID) { this.bucketID = bucketID; }
    public String getBucketID() { return bucketID; }
    public String getStreamID() { return streamID; }
    public void setStreamID(String streamID) { this.streamID = streamID; }
    public TreeMap<Date, String> getTimeSeries() {
        return timeSeries;
    }
    public TreeMap<Date, Complex[]> getPastDFTs() {
        return pastDFTs;
    }



}
