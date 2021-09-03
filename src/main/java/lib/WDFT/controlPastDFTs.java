package lib.WDFT;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class controlPastDFTs {

    private final int uID;
    private HashMap<String, pastListCoef> keyDFTs;
    private String[] param;
    private HashMap<String, String> mapGrid;
    HashMap<String, controlBucket> map;

    public controlPastDFTs(int uid, String[] parameters) {
        uID = uid;
        param = parameters;
        keyDFTs = new HashMap<>();
        mapGrid = new HashMap<>();
        map = new HashMap<>();
    }
    //Fill the map of <StreamID, pastListCoef>
    public void add(String data, String value , String streamID) {
        pastListCoef listCoef;
        if (!keyDFTs.containsKey(streamID)) {
            listCoef = new pastListCoef(uID, param, streamID);
        } else {
            listCoef = keyDFTs.get(streamID);
        }
        listCoef.add(data, value, streamID);
        keyDFTs.put(streamID, listCoef);
    }
    public void numberOfWindows() {
        for (Map.Entry<String, pastListCoef> value: keyDFTs.entrySet()) {
            System.out.println("Stream ID: " + value.getKey() + " Number of windows: " + value.getValue().getNumberOfWindows());
        }
    }
    //BucketID, Collection of windowPastDFTs
    public HashMap<String, controlBucket> estimate(double threshold) {
        mapGrid = gridToHash(threshold);
        sendToBuckets(threshold);
        return  map;
    }
    //HashMap -> <BucketKey, pastListCoef>
    private void sendToBuckets(double threshold) {
        initializeMap(threshold);
        for (Map.Entry<String, pastListCoef> entry : keyDFTs.entrySet()) {
            saveTheNeighbors(threshold, entry.getValue());
        }
    }
    //Initialization of the map
    private void initializeMap(double threshold) {
        int size = dimension(threshold);
        for (int i = 1; i <= size; i++) {
            map.put(String.valueOf(i), null);
        }
    }
    //Save the neighbors and send the data to the appropriate bucket in the map
    private void saveTheNeighbors(double threshold,  pastListCoef entry) {
        ArrayList<COEF> listOfCoefs = entry.getPastDFTs();
        String tmp = "COEFS -> ";
            for (COEF coef : listOfCoefs) {
                tmp = tmp + "\n COEF info -> "+ coef.getWindowTime() +"_"+coef.getStreamID();
                String[] s = coef.keyHash(threshold).split(",");
                // s =  1,2,3, ... maxBucketID
                // (y - 1)*maxX + x
                int bucketID = (int) ((Integer.parseInt(s[1]) - 1) * Math.sqrt(dimension(threshold)) + Integer.parseInt(s[0]));
                if (bucketID >= 1 && bucketID <= dimension(threshold)) {
                    String neighbors = mapGrid.get(String.valueOf(bucketID));
                    coef.setBucketID(String.valueOf(bucketID));
                    coef.setNeighbors(neighbors);
                    saveToFinalBuckets(threshold, coef);
                }
            }
           // System.out.println(tmp);
        }
    //Whether to create or not a new object of the class controlBucket.java
    private void saveToFinalBuckets(double threshold, COEF coef) {
        String bucketID = coef.getBucketID();
        if (map.get(bucketID) == null) {
            controlBucket cb = new controlBucket();
            cb.setBucketID(coef.getBucketID());
            map.put(bucketID, cb);
            cb = map.get(bucketID);
            addToBuckets(cb, coef, threshold);
        } else {
           controlBucket cb = map.get(bucketID);
           addToBuckets(cb, coef, threshold);
        }
    }
    //Policy: send to up, up right, down right, right
    private void addToBuckets(controlBucket cb, COEF coef, double threshold) {
        String[] neighbor = coef.getNeighbors().split(" ");
        cb.split(coef);
        map.put(coef.getBucketID(), cb);
        for (String s : neighbor) {
            controlBucket cbNeighbors = checkIfNull(s);
            int diff = Math.abs(Integer.parseInt(s) - Integer.parseInt(coef.getBucketID()));
            if ((diff == 1) && (Integer.parseInt(s) > Integer.parseInt(coef.getBucketID()))) {
                cbNeighbors.split(coef);
                map.put(s, cbNeighbors);
            } else if ((diff == (Math.sqrt(dimension(threshold)) + 1)) && (Integer.parseInt(s) > Integer.parseInt(coef.getBucketID()))) {
                cbNeighbors.split(coef);
                map.put(s, cbNeighbors);
            } else if ((diff == (Math.sqrt(dimension(threshold)) - 1)) && (Integer.parseInt(s) < Integer.parseInt(coef.getBucketID()))) {
                cbNeighbors.split(coef);
                map.put(s, cbNeighbors);
            } else if ((diff == Math.sqrt(dimension(threshold))) && (Integer.parseInt(s) > Integer.parseInt(coef.getBucketID()))) {
                cbNeighbors.split(coef);
                map.put(s, cbNeighbors);
            }
        }
    }
    //Check if it has been created an object of class controlBucket.java
    private controlBucket checkIfNull(String s) {
        if (map.get(s) == null) {
            controlBucket cb = new controlBucket();
            cb.setBucketID(s);
            map.put(s, cb);
            cb = map.get(s);
            return cb;
        }
        return map.get(s);
    }

    private int dimension(double threshold) {
        double epsilon = Math.sqrt(1 - threshold);
        int b = (int) Math.pow(Math.ceil(Math.sqrt(2) / epsilon), 2);
        return b;

    }
    //Calculation of the neighbors in the grid
    private HashMap<String, String> gridToHash(double threshold){
        double epsilon = Math.sqrt(1 - threshold);
        int dimensionOfArray = (int) Math.ceil(Math.sqrt(2) / epsilon);
        int[][] array = new int[dimensionOfArray][dimensionOfArray];
        int counter = 1;
        for (int i = 0; i < array.length; i++) {
            for (int j = 0; j < array.length; j++) {
                array[i][j] = counter;
                counter++;
            }
        }
        String list = "";
        HashMap<String, String> mapOfNeighbors = new HashMap<>();
        counter = 1;
        String points = "";
        for(int i = 0; i < array.length; i++) {
            for(int j = 0; j < array.length; j++) {
                points += String.valueOf(i).concat(",").concat(String.valueOf(j)).concat("\n");

            }
        }
        String[] xy = points.split("\n");
        for (String s : xy) {
            String[] tmp0 = s.split(",");
            int x0 = Integer.parseInt(tmp0[0]);
            int y0 = Integer.parseInt(tmp0[1]);
            for (String value : xy) {
                String[] tmp1 = value.split(",");
                int x1 = Integer.parseInt(tmp1[0]);
                int y1 = Integer.parseInt(tmp1[1]);
                double diff = Math.sqrt(Math.pow((x1 - x0), 2) + Math.pow((y1 - y0), 2));
                if (diff <= Math.sqrt(2) && diff > 0) {
                    list += String.valueOf(array[x1][y1]).concat(" ");
                }
            }
            mapOfNeighbors.put(String.valueOf(counter), list);
            counter++;
            list = "";
        }
        return mapOfNeighbors;
    }
}
