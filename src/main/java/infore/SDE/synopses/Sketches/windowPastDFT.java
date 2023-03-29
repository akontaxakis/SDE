package infore.SDE.synopses.Sketches;

import lib.PastCOEF.pastCOEF;
import lib.TimeSeries.windowDFT;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.LinkedList;

public class windowPastDFT {

    private Calendar c;
    private windowDFT ts;
    private pastCOEF pc;
    private DateFormat format;
    private Date currTime;
    private Date lastDate;
    private LinkedList<String> listSame;
    private int intervalSec;
    private int pointToSliding;

    public windowPastDFT(int uid, String[] parameters, String key) {
        pointToSliding = 0;
        lastDate = null;
        currTime = null;
        intervalSec = Integer.parseInt(parameters[3]);
        format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ENGLISH);
        c = Calendar.getInstance();
        listSame = new LinkedList<>();
        ts = new windowDFT(Integer.parseInt(parameters[4])/intervalSec, Integer.parseInt(parameters[5])/intervalSec,
                Integer.parseInt(parameters[6]), 1, key);
        pc = new pastCOEF();
    }
    //parameters: Date, Value
    public void add(String d, String v) {
        String date = d;
        String value = v;
        try {
            currTime = format.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if(lastDate == null){
            pc.getTimeSeries().put(currTime, value);
        }
        else {
            if (pc.getTimeSeries().containsKey(currTime)) {
                listSame.add(date.concat(value));//node->time and value
                pc.getTimeSeries().put(currTime, value); //if the value already exists, overwrite it.
            }
            else{
                pc.getTimeSeries().put(currTime, value); //in case that the key does not exist.
                long diff = (currTime.getTime() / 1000 - lastDate.getTime() / 1000);
                for(int i = 0; i < diff/intervalSec; i++){
                    if (!listSame.isEmpty()) {
                        ts.pushToValues(Double.parseDouble(pc.getTimeSeries().get(lastDate)));
                        listSame.removeAll(listSame);
                    }else{
                        ts.pushToValues(Double.parseDouble(value));
                    }
                    if(pointToSliding >= 35){
                        pc.getPastDFTs().put(lastDate, ts.getNormalizedFourierCoefficients().clone());
                    }
                    pointToSliding++;
                    if((diff > 1) && ((lastDate.getTime()/1000 + 1) < currTime.getTime()/1000)){
                        c.setTime(lastDate);
                        c.add(Calendar.SECOND, 1);
                        Date currentDatePlusOne = c.getTime();
                        pc.getTimeSeries().put(currentDatePlusOne, value);
                        lastDate = currentDatePlusOne;
                    }
                }
            }
        }
        lastDate = currTime;
    }
    //Calculation of BucketID
    public String keyHash(double threshold) {
        double epsilon = Math.sqrt(1 - threshold);
        int hashOffset = (int) Math.ceil(Math.sqrt(2) / 2);
        String stringKey = "";
        int tmpIndex;
        for (int i = 0; i < 8; i++) {
            tmpIndex = (int) Math.ceil((pc.getPastDFTs().lastEntry().getValue()[i].getReal() + hashOffset)/epsilon);
            stringKey += tmpIndex;
            stringKey += ",";

            tmpIndex = (int) Math.ceil(pc.getPastDFTs().lastEntry().getValue()[i].getImaginary() + hashOffset/epsilon);
            stringKey += tmpIndex;
            if (i < 8 - 1) {
                stringKey += ",";
            }
        }
        return stringKey;
    }
    //Calculation of the neighbors in the grid
    public HashMap<String, String> gridToHash(double threshold){
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

    public pastCOEF getPc() { return pc; }
}
