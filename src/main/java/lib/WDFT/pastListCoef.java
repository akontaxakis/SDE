package lib.WDFT;

import org.apache.commons.math3.complex.Complex;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class pastListCoef {

    private TreeMap<Date, String> filteredTimeSeries; //The original-filtered time series.
    private ArrayList<COEF> pastDFTs;
    private DFT dft;
    private DateFormat format;
    private Calendar c;
    private Calendar c1;
    private Date currentDatePlusOne;
    private Date currentDatePlusTen;
    private int intervalSec;
    private int pointToSliding;
    private Date currTime;
    private Date lastDate;
    private LinkedList<String> listSame;
    private int lengthOfWindow;
    private int slideOfWin;
    private String[] realValues;
    private int k;

   //KeyField, ValueField,  timeField,OperationMode,Interval in Seconds, Basic Window Size in Seconds, Sliding Window Size in Seconds,#coefficients,slide
    public pastListCoef(int uid, String[] parameters, String key) {
        filteredTimeSeries = new TreeMap<>();
        pastDFTs = new ArrayList<>();
        listSame = new LinkedList<>();
        format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ENGLISH);
        c = Calendar.getInstance();
        c1 = Calendar.getInstance();
        k = 0;
        intervalSec = Integer.parseInt(parameters[4]);
        lengthOfWindow = 1;
        pointToSliding = 0;
        dft = new DFT(Integer.parseInt(parameters[5]),Integer.parseInt(parameters[6]),Integer.parseInt(parameters[7]),1, key);
        slideOfWin = Integer.parseInt(parameters[8]);
        realValues = new String[slideOfWin];
    }

    public void add(String date, String value, String streamID) {
        //pushToValues(date,value, streamID);
        pushToValuesNew(date,value, streamID);
    }
    //parameters: Date, Value
    private void pushToValues(String date, String value, String streamID) {

        try {
            currTime = format.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        try {
            if (currTime.after(format.parse("01/01/2019 00:00:01"))) {
                if (lastDate == null) {
                    filteredTimeSeries.put(currTime, value);
                } else {
                    if (filteredTimeSeries.containsKey(currTime)) {
                        listSame.add(date.concat(value));//node->time and value
                        filteredTimeSeries.put(currTime, value); //if the value already exists, overwrite it.
                    } else {
                        filteredTimeSeries.put(currTime, value); //in case that the key does not exist.
                        long diff = (currTime.getTime() / 1000 - lastDate.getTime() / 1000);
                        currentDatePlusOne = currTime;
                        for (int i = 0; i < diff / intervalSec; i++) {
                            if (!listSame.isEmpty()) {
                                dft.pushToValues(Double.parseDouble(filteredTimeSeries.get(lastDate)));
                                listSame.removeAll(listSame);
                            } else {
                                dft.pushToValues(Double.parseDouble(value));
                            }
                            if (pointToSliding >= lengthOfWindow * slideOfWin) {
                                COEF cf = new COEF(currentDatePlusOne, streamID, dft.getNormalizedFourierCoefficients().clone(), realValues);
                                pastDFTs.add(cf);
                               //pastDFTs.add(addCoefficients(currTime, streamID);
                               //pastDFTs.get(sizeOfList()).setDft(dft.getNormalizedFourierCoefficients().clone());
                                lengthOfWindow++;
                                k = 0;
                                realValues = new String[slideOfWin];
                            }
                            c1.setTime(currentDatePlusOne);
                            c1.add(Calendar.SECOND, slideOfWin);
                            currentDatePlusOne = c1.getTime();
                            realValues[k] = value;
                            k++;
                            pointToSliding++;
                            if ((diff > 1) && ((lastDate.getTime() / 1000 + 1) < currTime.getTime() / 1000)) {
                                c.setTime(lastDate);
                                c.add(Calendar.SECOND, 1);
                                Date currentDatePlusOne = c.getTime();
                                filteredTimeSeries.put(currentDatePlusOne, value);
                                lastDate = currentDatePlusOne;
                            }
                        }
                    }
                }
                lastDate = currTime;
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void pushToValuesNew(String date, String value, String streamID) {
        try {
            currTime = format.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        try {
            if (currTime.after(format.parse("01/01/2019 00:00:01"))) {
                if (lastDate != null) {
                    long diff = (currTime.getTime() / 1000 - lastDate.getTime() / 1000)/60;
                    currentDatePlusTen = currTime;
                    for (int i = 0; i < diff / intervalSec; i++) {
                        dft.pushToValues(Double.parseDouble(value));
                        if (pointToSliding >= lengthOfWindow * slideOfWin) {
                            COEF cf = new COEF(currentDatePlusTen, streamID, dft.getNormalizedFourierCoefficients().clone(), realValues);
                           // System.out.println(Arrays.toString(cf.getDft()));
                            if (!checkIfNull(cf.getDft()))
                                pastDFTs.add(cf);
                            lengthOfWindow++;
                            k = 0;
                            realValues = new String[slideOfWin];
                        }
                        c1.setTime(currentDatePlusTen);
                        c1.add(Calendar.MINUTE, slideOfWin);
                        currentDatePlusTen = c1.getTime();
                        realValues[k] = value;
                        k++;
                        pointToSliding++;
                        if ((diff > 1) && ((lastDate.getTime() / (1000*60) + 1) < currTime.getTime() / (1000*60))) {
                            c.setTime(lastDate);
                            c.add(Calendar.MINUTE, 1);
                            currentDatePlusTen = c.getTime();
                            //filteredTimeSeries.put(currentDatePlusTen, value);
                            lastDate = currentDatePlusTen;
                        }
                    }
                }
                lastDate = currTime;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private boolean checkIfNull(Complex[] dft) {
        return dft[7].getReal()==0 && dft[7].getImaginary()==0;
    }

    private int sizeOfList() {
        return pastDFTs.size() - 1;
    }
    public int getNumberOfWindows() {
        return pastDFTs.size();
    }
    public TreeMap<Date, String> getFilteredTimeSeries() { return filteredTimeSeries; }
    public ArrayList<COEF> getPastDFTs() { return pastDFTs; }
}
