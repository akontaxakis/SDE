package lib.PastCOEF;

import infore.SDE.synopses.windowPastDFT;

import java.util.HashMap;
import java.util.Map;

public class pastListCoef {

    private HashMap<String, windowPastDFT> keyDFTs;//Stream ID and the object with the DFTs
    private HashMap<String, windowPastDFT> keyOriginalTimeSeries;//Stream ID and the object with the original time series
    private HashMap<String, Map.Entry<String, windowPastDFT>> mapWithBucketID;

    public pastListCoef() {
        keyDFTs = new HashMap<>();
        keyOriginalTimeSeries = new HashMap<>();
        mapWithBucketID = new HashMap<>();
    }

    public HashMap<String, windowPastDFT> getKeyOriginalTimeSeries() { return keyOriginalTimeSeries; }
    public void setKeyOriginalTimeSeries(HashMap<String, windowPastDFT> keyOriginalTimeSeries) { this.keyOriginalTimeSeries = keyOriginalTimeSeries; }
    public HashMap<String, windowPastDFT> getKeyDFTs() { return keyDFTs; }
    public void setKeyDFTs(HashMap<String, windowPastDFT> keyDFTs) { this.keyDFTs = keyDFTs; }
    public HashMap<String, Map.Entry<String, windowPastDFT>> getMapWithBucketID() { return mapWithBucketID; }
    public void setMapWithBucketID(HashMap<String, Map.Entry<String, windowPastDFT>> mapWithBucketID) { this.mapWithBucketID = mapWithBucketID; }
}
