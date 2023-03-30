package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import javafx.util.Pair;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

public class Test_Return_FIN_Data {

    private static final DecimalFormat df = new DecimalFormat("00.000");

    public static void main(String[] args) throws Exception {


        //SendaddRequest("RAD_REQUEST_5");
        //sendFINPrices("RAD_DATA_PR_4");
        sendREData("RAD_RR_4");
    }

    public static void sendFINData(String kafkaDataInputTopic) throws IOException {
        String folderPath = "C:\\Users\\adoko\\Downloads\\FinancialData\\history";
        String line = "";
        String topicName = kafkaDataInputTopic;
        int nOfMessages = 0;
        final File folder = new File(folderPath);
        ArrayList<Pair<String, BufferedReader>> br = new ArrayList<Pair<String, BufferedReader>>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int window = 256;

        for (File fileEntry : folder.listFiles()) {
            int i = 0;
            String W = "";
            String previous=";";
            if (fileEntry.getName().endsWith("his")) {

                BufferedReader br1 = new BufferedReader(new FileReader(fileEntry.getAbsolutePath()));
                String k = fileEntry.getName().toString().replace(".his", "");
                String stock = k;
                stock = stock.replace("╖", "");
                stock = stock.replace("·", "");
                br.add(new Pair<String, BufferedReader>(stock, br1));
                while ((line = br1.readLine()) != null) {

                    String stock2 = stock.replace(" ", "");
                    stock2 = stock2.replace("╖", "");
                    stock2 = stock2.replace("·", "");

                    String[] words = new String[4];
                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                    //System.out.println(line);
                    if (tokenizer.hasMoreTokens()) {
                        for (int jk = 0; jk < 4; jk++) {
                            words[jk] = tokenizer.nextToken();
                        }

                    if (i < window+1) {
                        if(i>0) {
                            if (i < window) {
                                double diff=Double.parseDouble(previous)-Double.parseDouble(words[2]);
                                W = W +  df.format(diff) + ";";
                            } else {
                                double diff=Double.parseDouble(previous)-Double.parseDouble(words[2]);
                                W = W +  df.format(diff);
                            }
                        }
                        previous = words[2];
                        i++;
                    }else{
                        i=0;
                        System.out.println(line);
                        stock2=stock2 + words[1].replace(":","");
                        String jsonString = "{\"StockID\":\"" + stock2+ "\",\"price\":\"" +W + "\"}";
                        W = "";
                        String str = "" + words[0] + " " + words[1];
                        //data string
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode node = mapper.readTree(jsonString);
                        Datapoint dp = new Datapoint("W_FIN_USECASE", stock2, node);

                        //SDE data string
                        System.out.println(dp.toJsonString());
                        //producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));
                        nOfMessages++;
                    }
                    }
                }
            }
        }
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }

    public static void SendaddRequest(String topicRequests) throws JsonProcessingException {

        //STOCK ID, RETURN, #GROUPS, GROUP_DIMENSIONS, SketchSIZE, windowSize, threshold
        //String[] parameters5 = {"StockID", "price", "60", "2", "60","256","50"};

        //STOCK ID, RETURN, #GROUPS, GROUP_DIMENSIONS, SketchSIZE, windowSize, threshold
        String[] parameters5 = {"StockID", "price", "5", "2", "10","256","70"};


        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //Set acknowledgements for producer requests.
        props.put("acks", "all");
        //If the request fails, the producer can automatically retry,
        props.put("retries", 0);
        //Specify buffer size in config
        props.put("batch.size", 16384);
        //Reduce the no of requests less than 0
        props.put("linger.ms", 0);
        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        Request rq = new Request("W_FIN_USECASE", 5, 100, 1110, "NONE", parameters5, 4);


        producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));
        System.out.println(rq.toJsonString());
        producer.close();
    }

    public static void sendFINPrices(String kafkaDataInputTopic) throws IOException {
        String folderPath = "C:\\Users\\adoko\\Downloads\\FinancialData\\history";
        String line = "";
        String topicName = kafkaDataInputTopic;
        int nOfMessages = 0;
        final File folder = new File(folderPath);
        ArrayList<Pair<String, BufferedReader>> br = new ArrayList<Pair<String, BufferedReader>>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int window = 256;

        for (File fileEntry : folder.listFiles()) {
            int i = 0;
            String W = "";
            if (fileEntry.getName().endsWith("his")) {

                BufferedReader br1 = new BufferedReader(new FileReader(fileEntry.getAbsolutePath()));
                String k = fileEntry.getName().toString().replace(".his", "");
                String stock = k;
                stock = stock.replace("╖", "");
                stock = stock.replace("·", "");
                br.add(new Pair<String, BufferedReader>(stock, br1));
                while ((line = br1.readLine()) != null) {

                    String stock2 = stock.replace(" ", "");
                    stock2 = stock2.replace("╖", "");
                    stock2 = stock2.replace("·", "");

                    String[] words = new String[4];
                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                    //System.out.println(line);
                    if (tokenizer.hasMoreTokens()) {
                        for (int jk = 0; jk < 4; jk++) {
                            words[jk] = tokenizer.nextToken();
                        }

                        if (i < window) {
                            if(i>0) {
                                if (i < window-1) {
                                    W = W +  words[2] + ";";
                                } else {
                                    W = W +  words[2];
                                }
                            }

                            i++;
                        }else{
                            i=0;
                            //System.out.println(line);
                            stock2=stock2 + words[1].replace(":","");
                            String jsonString = "{\"StockID\":\"" + stock2+ "\",\"price\":\"" +W + "\"}";
                            W = "";
                            String str = "" + words[0] + " " + words[1];
                            //data string
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode node = mapper.readTree(jsonString);
                            Datapoint dp = new Datapoint("W_FIN_USECASE", stock2, node);

                            //SDE data string
                            //System.out.println(dp.toJsonString());
                            producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));
                            nOfMessages++;
                        }
                    }
                }
            }
        }
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }


    public static void sendREData(String kafkaDataInputTopic) throws IOException {
        String folderPath = "C:\\Users\\adoko\\Downloads\\FinancialData\\history";
        String line = "";
        String topicName = kafkaDataInputTopic;
        int nOfMessages = 0;
        final File folder = new File(folderPath);
        ArrayList<Pair<String, BufferedReader>> br = new ArrayList<Pair<String, BufferedReader>>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        int window = 256;

        for (File fileEntry : folder.listFiles()) {
            int i = 0;
            String W = "";
            String previous=";";
            if (fileEntry.getName().endsWith("his")) {

                BufferedReader br1 = new BufferedReader(new FileReader(fileEntry.getAbsolutePath()));
                String k = fileEntry.getName().toString().replace(".his", "");
                String stock = k;
                stock = stock.replace("╖", "");
                stock = stock.replace("·", "");
                br.add(new Pair<String, BufferedReader>(stock, br1));
                while ((line = br1.readLine()) != null) {

                    String stock2 = stock.replace(" ", "");
                    stock2 = stock2.replace("╖", "");
                    stock2 = stock2.replace("·", "");

                    String[] words = new String[4];
                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                    //System.out.println(line);
                    if (tokenizer.hasMoreTokens()) {
                        for (int jk = 0; jk < 4; jk++) {
                            words[jk] = tokenizer.nextToken();
                        }

                        if (i < window+1) {
                            if(i>0) {
                                if (i < window) {
                                    double diff=(Double.parseDouble(words[2])/Double.parseDouble(previous))-1;
                                    W = W +  diff+ ";";
                                    //W = W +  df.format(diff) + ";";
                                } else {
                                    double diff=(Double.parseDouble(words[2])/Double.parseDouble(previous))-1;
                                    W = W +  diff;
                                }
                            }
                            previous = words[2];
                            i++;
                        }else{
                            i=0;
                            System.out.println(line);
                            stock2=stock2 + words[1].replace(":","");
                            String jsonString = "{\"StockID\":\"" + stock2+ "\",\"price\":\"" +W + "\"}";
                            W = "";
                            String str = "" + words[0] + " " + words[1];
                            //data string
                            ObjectMapper mapper = new ObjectMapper();
                            JsonNode node = mapper.readTree(jsonString);
                            Datapoint dp = new Datapoint("W_FIN_USECASE", stock2, node);

                            //SDE data string
                            //System.out.println(dp.toJsonString());
                            producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));
                            nOfMessages++;
                        }
                    }
                }
            }
        }
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }


}
