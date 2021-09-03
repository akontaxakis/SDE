package lib.WDFT;

import java.io.*;
import java.util.*;

public class correlationPastDFT {

    private final int numOfWorkers;
    private int counter;
    private controlBucket cb;
    private double threshold;
    private int K;
    private int T;
    private String stocks;
    private int n;
    private String Key;


    public correlationPastDFT(int workers, double th, int k, int t, String stockN, int numQ, String key) {
        counter = 0;
        numOfWorkers = workers;
        cb = new controlBucket(key, k, th, t);
        threshold = th;
        K = k;
        T = t;
        stocks = stockN;
        n = numQ;
        Key = key;
    }

    public boolean merge(controlBucket bucketToAdd) {
        counter++;
        if (bucketToAdd != null)
            cb.add(bucketToAdd);
        return counter == numOfWorkers;
    }

    public Object reduce() {
        LinkedList<PAIR> obj = new LinkedList<>();
        if (n == 1) {
            System.out.println("####################Query 1##########################");
            HashMap<String, LinkedList<PAIR>> obj1 = cb.query_1(threshold, K, stocks.split(";"), T);
            writeHashMapToFile(obj1);
            return obj1;
        } else if (n == 2) {
            System.out.println("####################Query 2###########################");
            obj = cb.query_2(threshold, K, stocks.split(";"), T);
        } else if (n == 3) {
            System.out.println("####################Query 3###########################");
            obj = cb.query_3(threshold, K);
        } else if (n == 4) {
            System.out.println("####################Query 4###########################");
            obj = cb.query_4(threshold, K);
        } else if (n == 5) {
            System.out.println("####################Query 5###########################");
            obj = cb.query_5(threshold, K, T, stocks.split(";"));
        }
        return obj;
    }

    public void writeHashMapToFile(HashMap<String, LinkedList<PAIR>> map) {
        String path = "/Users/christinamanara/Desktop/Versions/Data/";
        System.out.println("Hrere" + map.get("ForexAUDJPYNoExpiry"));
        try {
            //   for (Map.Entry<String, LinkedList<PAIR>> entry : map.entrySet()) {
            File myWriter = new File(path+"output");
            //System.out.println("here");
            FileWriter writer = new FileWriter(myWriter, true);
            BufferedWriter writeToFile = new BufferedWriter(writer);
            writeToFile.write(String.valueOf(map));
//                writer.append(entry.getKey());
//                writer.append('\n');
//                writer.append(entry.getValue().toString());
            writeToFile.close();
            //  }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


       /* File myWriter = new File(path);
        try {
            if (myWriter.createNewFile()) {
                System.out.println("here");
                System.out.println(myWriter.getAbsolutePath());
                FileWriter writer = new FileWriter(myWriter, true);
                BufferedWriter writeToFile = new BufferedWriter(writer);
                for (Map.Entry<String, LinkedList<PAIR>> entry : map.entrySet()) {
                    writeToFile.append(entry.getKey());
                    writeToFile.append('\n');
                    writeToFile.append(entry.getValue().toString());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }*/

    }
}
