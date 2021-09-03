package lib.WDFT;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalTime;
import java.util.*;

public class main {

    public static void main(String[] args) throws ParseException {

        String[] p = {"StockID","price","DateTime","1","1","20","100","8","100"};
        controlPastDFTs add = new controlPastDFTs(1, p);

        String path = "/Users/christinamanara/Desktop/Versions/Data/data1/data";

        File folder = new File(path);
        File[] listOfFiles = folder.listFiles();

        assert listOfFiles != null;
        for (File file : listOfFiles) {
            System.out.println(file.getName());
            if (file.isFile()) {
                try {
                    Scanner myReader = new Scanner(file);
                    while (myReader.hasNextLine()) {
                        String data = myReader.nextLine();

                        String[] parameters = data.split(",");
                        String date = parameters[0].concat(" ").concat(parameters[1]);
                        String value = parameters[2];
                        add.add(date, value, file.getName());
                    }
                    myReader.close();
                } catch (FileNotFoundException e) {
                    System.out.println("An error occurred.");
                    e.printStackTrace();
                }
            }
        }

        //Calculation of final buckets with the appropriate arraylists
        LocalTime startTime = LocalTime.now();
        HashMap<String, controlBucket> m = add.estimate(0.8);
        System.out.println("Estimate Duration time: " + Duration.between(startTime, LocalTime.now()).toMinutes());

        //System.out.println(m);
        //Correlation
//        correlationPastDFT cor = new correlationPastDFT(8);
//        for (Map.Entry<String, Collection<windowPastDFT>> entry : m.entrySet()){
//            ArrayList<windowPastDFT> newList = new ArrayList<>();
//            newList = entry.getValue().stream().collect(toCollection(ArrayList::new));
//            cor.split(newList, entry.getKey());
//        }
        //add.numberOfWindows();
        String[] tmp = {"CryptosBTCEURNoExpiry", "ForexAUDJPYNoExpiry"};
        DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ENGLISH);
        System.out.println("------------------------------------------------------------");
        LocalTime startTime1 = LocalTime.now();
        for (Map.Entry<String, controlBucket> entry : m.entrySet()) {
            //m.get(String.valueOf(3)).correlate(0.7);
            //System.out.println("Here");
            System.out.println("\n Bucket: " + entry.getKey() + "\n");
            if (entry.getValue() != null)
                entry.getValue().correlate(0.9, 10, tmp, 2, 1);

            System.out.println("------------------------------------------------------------");
        }
        System.out.println("Correlate Duration time: " + Duration.between(startTime1, LocalTime.now()).toMinutes());
    }
}
