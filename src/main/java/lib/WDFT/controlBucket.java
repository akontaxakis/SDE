package lib.WDFT;

import org.apache.commons.math3.complex.Complex;
import org.apache.kafka.common.protocol.types.Field;

import java.io.*;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class controlBucket {

    private LinkedList<COEF> nativeList;
    private LinkedList<COEF> neighborsList;
    private String bucketID;
    private correlatedPairs cp;
    private double Th;
    private int t;
    private int K;

    public controlBucket() {
        nativeList = new LinkedList<>();
        neighborsList = new LinkedList<>();
        cp = new correlatedPairs();
    }

    public controlBucket(String key, int k, double threshold, int T) {
        Th = threshold;
        t = T;
        K = k;
        bucketID = key;
        nativeList = new LinkedList<>();
        neighborsList = new LinkedList<>();
        cp = new correlatedPairs();
    }

    public void add(controlBucket cb) {
        nativeList.addAll(cb.nativeList);
        neighborsList.addAll(cb.neighborsList);
    }

    public void split(COEF coef) {
        if (coef.getBucketID().equals(bucketID)) {
            nativeList.add(coef);
        } else {
            neighborsList.add(coef);
        }
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
    public Object correlate(double threshold, int k, String[] stockN, int T, int n) {
        LinkedList<PAIR> obj = new LinkedList<>();
        if (n == 1) {
            System.out.println("####################Query 1##########################");
            HashMap<String, LinkedList<PAIR>> obj1 = query_1(threshold, k, stockN, T);
            writeHashMapToFile(obj1);
            return obj1;
        } else if (n == 2) {
            System.out.println("####################Query 2###########################");
            obj = query_2(threshold, k, stockN, T);
        } else if (n == 3) {
            System.out.println("####################Query 3###########################");
            obj = query_3(threshold, k);
        } else if (n == 4) {
            System.out.println("####################Query 4###########################");
            obj = query_4(threshold, k);
        } else if (n == 5) {
            System.out.println("####################Query 5###########################");
            obj = query_5(threshold, k, T, stockN);
        }
        return obj;
    }

    public HashMap<String, LinkedList<PAIR>> query_1(double threshold, int k, String[] stockN, int T) {
        HashMap<String, LinkedList<PAIR>> coefFinal = new HashMap<>();
        HashMap<String, LinkedList<COEF>> coef1 = findStocksInNative1(stockN);
        correlatedPairs cpn;
        for (Map.Entry<String, LinkedList<COEF>> entry : coef1.entrySet()) {
            cpn = new correlatedPairs();
            similarityGivenStocks(threshold, nativeList, entry.getValue(), (int) TimeUnit.MINUTES.toMillis(T), cpn);
            similarityGivenStocks(threshold, neighborsList, entry.getValue(), (int) TimeUnit.MINUTES.toMillis(T), cpn);
            LinkedList<PAIR> topK = new LinkedList<>();
            topK = cpn.topK(k, threshold);
            if (topK != null) {
                for (PAIR pair : topK) {
                    System.out.println("Top K: " + pair.toString());
                }
            }
            coefFinal.put(entry.getKey(), topK);
        }
        return coefFinal;
    }
    
    private HashMap<String, LinkedList<COEF>> findStocksInNative1(String[] stockName) {
        HashMap<String, LinkedList<COEF>> coefs = new HashMap<>();
        LinkedList<COEF> coefs1 =  new LinkedList<>();
        for (String stock : stockName) {
            for (COEF coef : nativeList) {
                if (coef.getStreamID().equals(stock)) {
                    coefs1.add(coef);
                }
            }
            coefs.put(stock, coefs1);
            coefs1 =  new LinkedList<>();
        }
        return coefs;
    }

    public LinkedList<PAIR> query_2(double threshold, int k, String[] stockN, int T) {
        LinkedList<PAIR> topKList = new LinkedList<>();
        LinkedList<LinkedList<COEF>> coef2 = findStocksInNative2(stockN);
        int i = 0;
        for (LinkedList<COEF> c1 : coef2) {
            i++;
            int j = 0;
            for (LinkedList<COEF> c2 : coef2) {
                if (!c1.equals(c2) && i > j) {
                    similarityGivenStocks(threshold, c1, c2, (int) TimeUnit.MINUTES.toMillis(T), cp);
                    topKList = cp.topK(k, threshold);
                    if (topKList != null) {
                        for (PAIR pair : topKList) {
                            System.out.println("Top K: " + pair.toString());
                        }
                    }
                }
                j++;
            }
        }
        return topKList;
    }

    private LinkedList<LinkedList<COEF>> findStocksInNative2(String[] stockName) {
        LinkedList<COEF> coefs = new LinkedList<>();
        LinkedList<LinkedList<COEF>> listCoef = new LinkedList<>();
        for (String stock : stockName) {
            for (COEF coef : nativeList) {
                if (coef.getStreamID().equals(stock)) {
                    coefs.add(coef);
                }
            }
            for (COEF coef : neighborsList) {
                if (coef.getStreamID().equals(stock)) {
                    coefs.add(coef);
                }
            }
            listCoef.add(coefs);
            coefs = new LinkedList<>();
        }
        return listCoef;
    }

    private void similarityGivenStocks(double threshold, LinkedList<COEF> listA, LinkedList<COEF> coefficients, int T, correlatedPairs cpN) {
        double correlation;
        PAIR pr;
        boolean f1 = false, f2 = false, f3 = false, f4 = false, f5 = false;
        LocalTime startTime = LocalTime.now();
        int  i = 0;
        System.out.println("--------------Find the similar stocks based on a list--------------");
        System.out.println("Size of list: " + listA.size());
        for (COEF wp2 : coefficients) {
            String stockName = wp2.getStreamID();
            Complex[] d_wp2 = wp2.getDft();
            for (COEF wp1 : listA) {
                if ((!f1) && i >= listA.size() * 0.01) {
                    System.out.println("above 1%");
                    f1 = true;
                }
                if ((!f2) && i >= listA.size() * 0.10) {
                    f2 = true;
                    System.out.println("above 10%");
                }
                if ((!f3) && i >= listA.size() * 0.20) {
                    f3 = true;
                    System.out.println("above 20%");
                }
                if ((!f4) && i >= listA.size() * 0.50) {
                    f4 = true;
                    System.out.println("above 50%");
                }
                if ((!f5) && i >= listA.size() * 0.70) {
                    f5 = true;
                    System.out.println("above 70%");
                }
                i++;
                String streamID1 = wp1.getStreamID();
                Complex[] d_wp1 = wp1.getDft();
                long timeInterval = Math.abs(wp1.getWindowTime().getTime() - wp2.getWindowTime().getTime());
                //System.out.println("Start time " + wp1.getWindowTime() + " last time " + wp2.getWindowTime() + " time dif " + timeInterval);
                if (!streamID1.equals(stockName) && timeInterval <= T) {
                    correlation = corr(d_wp1, d_wp2);

                    if (correlation > threshold && correlation <= 1) {
                        pr = new PAIR(correlation, streamID1, stockName, wp1, wp2);
                        cpN.addToMap(pr);
                    }
                }
            }
        }
        System.out.println("Duration time: " + Duration.between(startTime, LocalTime.now()).toMinutes());
    }

    public LinkedList<PAIR> query_3(double threshold, int k) {
        similarityAll(threshold, nativeList, nativeList);
        similarityAll(threshold, nativeList, neighborsList);
        LinkedList<PAIR> topK = cp.topK(k, threshold);
        System.out.println("Top K: " + topK);
        return topK;
    }

    private void similarityAll(double threshold, LinkedList<COEF> listA, LinkedList<COEF> listB) {
        double correlation;
        PAIR pr;
        int  i = 0;
        System.out.println("--------------Compare all windows--------------");
        System.out.println("Size of native list: " + listA.size() + " Size of neighbors list: " + listB.size());
        boolean f1 = false, f2 = false, f3 = false, f4 = false, f5 = false;
        LocalTime startTime = LocalTime.now();
        for (COEF wp1 : listA) {
            if ((!f1) && i >= listA.size() * 0.01) {
                System.out.println("above 1%");
                f1 = true;
            }
            if ((!f2) && i >= listA.size() * 0.10) {
                f2 = true;
                System.out.println("above 10%");
            }
            if ((!f3) && i >= listA.size() * 0.20) {
                f3 = true;
                System.out.println("above 20%");
            }
            if ((!f4) && i >= listA.size() * 0.50) {
                f4 = true;
                System.out.println("above 50%");
            }
            if ((!f5) && i >= listA.size() * 0.70) {
                f5 = true;
                System.out.println("above 70%");
            }
            i++;
            int j = 0;
            String streamID1 = wp1.getStreamID();
            Complex[] d_wp1 = wp1.getDft();
            for (COEF wp2 : listB) {
                if (j > i) {
                    String streamID2 = wp2.getStreamID();
                    if (!(streamID1.equals(streamID2))) {
                        correlation = corr(d_wp1, wp2.getDft());

                        if (correlation > threshold && correlation <= 1) {

                            pr = new PAIR(correlation, streamID1, streamID2, wp1, wp2);
                            cp.addToMap(pr);
                        }
                    }
                }
                j++;
            }
        }
        System.out.println("Duration time: " + Duration.between(startTime, LocalTime.now()).toMinutes());
    }

    public LinkedList<PAIR> query_4(double threshold, int k) {
        similaritySameWin(threshold, nativeList, nativeList);
        similaritySameWin(threshold, nativeList, neighborsList);
        LinkedList<PAIR> topKSameWin = cp.topKSameWin(k, threshold);
        System.out.println("Top k list on same window: " + topKSameWin);
        return topKSameWin;
    }

    private void similaritySameWin(double threshold, LinkedList<COEF> listA, LinkedList<COEF> listB) {
        double correlation;
        PAIR pr;
        int  i = 0;
        System.out.println("--------------Compare on same windows--------------");
        System.out.println("Size of native list: " + listA.size() + " Size of neighbors list: " + listB.size());
        boolean f1 = false, f2 = false, f3 = false, f4 = false, f5 = false;
        for (COEF wp1 : listA) {
            if ((!f1) && i >= listA.size() * 0.01) {
                System.out.println("above 1%");
                f1 = true;
            }
            if ((!f2) && i >= listA.size() * 0.10) {
                f2 = true;
                System.out.println("above 10%");
            }
            if ((!f3) && i >= listA.size() * 0.20) {
                f3 = true;
                System.out.println("above 20%");
            }
            if ((!f4) && i >= listA.size() * 0.50) {
                f4 = true;
                System.out.println("above 50%");
            }
            if ((!f5) && i >= listA.size() * 0.70) {
                f5 = true;
                System.out.println("above 70%");
            }
            i++;
            int j = 0;
            Date win1 = wp1.getWindowTime();
            String streamID1 = wp1.getStreamID();
            Complex[] d_wp1 = wp1.getDft();
            for (COEF wp2 : listB) {
                if (j > i || !listA.equals(listB)) {
                    Date win2 = wp2.getWindowTime();
                    String streamID2 = wp2.getStreamID();
                    if ((win1.equals(win2)) && !(streamID1.equals(streamID2))) {
                        correlation = corr(d_wp1, wp2.getDft());

                        if (correlation > threshold && correlation <= 1) {
                            pr = new PAIR(correlation, streamID1, streamID2, wp1, wp2);
                            cp.addOnSameWin(pr);
                        }
                    }
                }
                j++;
            }
        }
    }

    public LinkedList<PAIR> query_5(double threshold, int k, int T, String[] stockN) {
        LinkedList<COEF> coef2 = findLastWindow(stockN);
        similarityLastWin(threshold, (int) TimeUnit.MINUTES.toMillis(T), coef2, nativeList);
        similarityLastWin(threshold, (int) TimeUnit.MINUTES.toMillis(T), coef2, neighborsList);
        LinkedList<PAIR> topK = cp.topK(k, threshold);
        if (topK != null) {
            for (PAIR pair : topK) {
                System.out.println("Top K: " + pair.toString());
            }
        }
        return topK;
    }

    private LinkedList<COEF> findLastWindow(String[] stockN) {
        LinkedList<COEF> foundCoef = new LinkedList<>();
        for (String stock : stockN) {
            if (nativeList.size() != 0 && neighborsList.size() != 0) {
                COEF coef = lastWind(nativeList, stock);
                COEF coef1 = lastWind(neighborsList, stock);
                if (coef != null && coef1 != null) {
                    if (coef.getWindowTime().after(coef1.getWindowTime()))
                        foundCoef.add(coef);
                    else
                        foundCoef.add(coef1);
                } else if (coef != null) {
                    foundCoef.add(coef);
                } else if (coef1 != null) {
                    foundCoef.add(coef1);
                }
            } else if (nativeList.size() != 0) {
                COEF coef = lastWind(nativeList, stock);
                assert coef != null;
                foundCoef.add(coef);
            } else if (neighborsList.size() != 0) {
                COEF coef1 = lastWind(neighborsList, stock);
                assert coef1 != null;
                foundCoef.add(coef1);
            }
        }
        return foundCoef;
    }

    private COEF lastWind(LinkedList<COEF> list, String stock){
        Date past = null, cur;
        COEF coefFinal = null;
        for (COEF coef : list) {
            if (stock.equals(coef.getStreamID())) {
                cur = coef.getWindowTime();
                if (past == null) {
                    past = cur;
                } else {
                    if (cur.after(past)) {
                        past = cur;
                        coefFinal = coef;
                    }
                }
            }
        }
        return coefFinal;
    }

    private void similarityLastWin(double threshold, int T, LinkedList<COEF> coef2, LinkedList<COEF> listA) {
        double correlation;
        PAIR pr;
        boolean f1 = false, f2 = false, f3 = false, f4 = false, f5 = false;
        LocalTime startTime = LocalTime.now();
        int  i = 0;
        System.out.println("--------------Find the similar stocks based on a list--------------");
        System.out.println("Size of list: " + listA.size());
        for (COEF wp2 : coef2) {
            String stockName = wp2.getStreamID();
            Complex[] d_wp2 = wp2.getDft();
            for (COEF wp1 : listA) {
                if ((!f1) && i >= listA.size() * 0.01) {
                    System.out.println("above 1%");
                    f1 = true;
                }
                if ((!f2) && i >= listA.size() * 0.10) {
                    f2 = true;
                    System.out.println("above 10%");
                }
                if ((!f3) && i >= listA.size() * 0.20) {
                    f3 = true;
                    System.out.println("above 20%");
                }
                if ((!f4) && i >= listA.size() * 0.50) {
                    f4 = true;
                    System.out.println("above 50%");
                }
                if ((!f5) && i >= listA.size() * 0.70) {
                    f5 = true;
                    System.out.println("above 70%");
                }
                i++;
                String streamID1 = wp1.getStreamID();
                Complex[] d_wp1 = wp1.getDft();
                long timeInterval = Math.abs(wp1.getWindowTime().getTime() - wp2.getWindowTime().getTime());
                //System.out.println("Start time " + wp1.getWindowTime() + " last time " + wp2.getWindowTime() + " time dif " + timeInterval);
                if (!streamID1.equals(stockName) && timeInterval <= T) {
                    correlation = corr(d_wp1, d_wp2);

                    if (correlation > threshold && correlation <= 1) {
                        pr = new PAIR(correlation, streamID1, stockName, wp1, wp2);
                        cp.addToMap(pr);
                    }
                }
            }
        }
        System.out.println("Duration time: " + Duration.between(startTime, LocalTime.now()).toMinutes());
    }

    private double distance(Complex[] a, Complex[] b) {
        double distance = 0;
        for (int i = 1; i < a.length; i++) {
            distance += Math.pow((a[i].getReal() - b[i].getReal()), 2);
            distance += Math.pow((a[i].getImaginary() - b[i].getImaginary()), 2);
        }
        return distance;
    }

    private double corr(Complex[] a, Complex[] b) {
        double distance = distance(a,b);
        return 1 - Math.pow(distance, 2) / 2;
    }

    public String getBucketID() { return bucketID; }

    public void setBucketID(String bucketID) { this.bucketID = bucketID; }

    @Override
    public String toString() {
        return "Bucket{" +
                "BucketID='" + this.bucketID + '\'' +
                ", NativeList=" + nativeList.size() +
                ", NeighborsList=" + neighborsList.size() +
                ", Th=" + this.Th +
                ", T=" + this.t +
                ", K=" + this.K +
                '}';
    }
}
