package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.*;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public class sendFINTest {


    public void run(String kafkaDataInputTopic) {
        try {
            SendaddRequest();
            //sendFINData(kafkaDataInputTopic);
            TimeUnit.SECONDS.sleep(3);
            SendEstimateRequest();

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }



    private static void SendaddRequest() throws JsonProcessingException {

        String topicRequests = "Rq_Fin";
        String[] parameters5 = {"StockID","price","DateTime","1","1","50","300","8","200"};;
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
        //props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        //props.put("bootstrap.servers", "159.69.32.166:9092");
        // http://2.84.152.37/
        //props.put("bootstrap.servers","2.84.152.37:9092,2.84.152.37:9094,2.84.152.37:9095");
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



        Request rq = new Request("fin_useCase", 1, 29, 1110, "INTEL", parameters5, 4);


        //producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));
        System.out.println(rq.toJsonString());
        producer.close();
    }

    private static void SendEstimateRequest() throws JsonProcessingException {

        String topicRequests = "Rq_Fin";
        String[] parameters2 = {"Query_ID","0.8","15,id1;id2"};
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9092");
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




        Request rq = new Request("fin_useCase", 3, 29, 1110, "INTEL", parameters2, 4);

        //producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));

        producer.close();
    }
/*
    private static void sendFINData(String kafkaDataInputTopic) throws IOException {
        String folderPath = "D:\\INFORE-data\\Forex\\Data";
        String line = "";
        String topicName = kafkaDataInputTopic;

        int nOfMessages = 0;

        final File folder = new File(folderPath);
        ArrayList<Pair<String, BufferedReader>> br = new ArrayList<Pair<String, BufferedReader>>();
        Properties props = new Properties();
        //props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        props.put("bootstrap.servers","localhost:9092");
        //props.put("bootstrap.servers","45.10.26.123:19092,45.10.26.123:29092,45.10.26.123:39092");

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

        //BufferedReader  br = new BufferedReader(new FileReader("C:\\Users\\ado.kontax\\Desktop\\data\\InforeWP2\\DataAPI_Source_code_Java\\DataAPI_Source_code_Java\\kafkaspringapi\\src\\springI1"));

            for (File fileEntry : folder.listFiles()) {
                if (fileEntry.isDirectory()) {

                    System.out.println(fileEntry.getAbsolutePath());

                    for (File fileEntry2 : fileEntry.listFiles()) {

                        BufferedReader br1 = new BufferedReader(new FileReader(fileEntry2.getAbsolutePath()));
                        String k = fileEntry2.getName().toString().replace(".txt", "");
                        String stock = k;
                        stock = stock.replace("╖", "");
                        stock = stock.replace("·", "");
                        br.add(new Pair<String, BufferedReader>(stock, br1));

                        int c = 0;
                        int size = br.size();

                        while (c < size) {
                            Iterator<Pair<String, BufferedReader>> e = br.iterator();

                            while (e.hasNext()) {

                                Pair<String, BufferedReader> br2 = e.next();
                                line = br2.getValue().readLine();

                                String stock2 = br2.getKey().replace(" ", "");
                                stock2 = stock2.replace("╖", "");
                                stock2 = stock2.replace("·", "");


                                if (line == null) {
                                    c++;
                                    break;

                                } else {

                                    String[] words = new String[4];
                                    StringTokenizer tokenizer = new StringTokenizer(line, ",");
                                    //System.out.println(line);
                                    for (int jk = 0; jk < 4; jk++) {
                                        words[jk] = tokenizer.nextToken();
                                    }

                                    if( stock2.startsWith("Forex")) {

                                        String jsonString = "{\"DateTime\":\"" + words[0] + " " + words[1] + "\",\"StockID\":\"" + stock2 + "\",\"price\":\"" + words[2] +"\",\"Volume\":\"" + words[3] + "\"}";
                                        String str= "" + words[0] + " " + words[1] ;
                                        //data string
                                        //System.out.println(jsonString);
                                        ObjectMapper mapper = new ObjectMapper();
                                        JsonNode node = mapper.readTree(jsonString);
                                        //String phoneType = node.get("phonetype").asText();
                                        //String cat = node.get("cat").asText();


                                        Datapoint dp = new Datapoint("fin_useCase", stock2, node);
                                        //Datapoint dp =  new Datapoint("Forex", stock2,words[0]+" "+words[1]+","+words[2]);
                                        // producer.send(new ProducerRecord<String, String>(topicName, jsonString));

                                        //SDE data string
                                        //System.out.println(dp.toJsonString());
                                        //producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));

                                        //producer.send(new ProducerRecord<>(topicName, dp.toJsonString()));
                                        // producer.send(new ProducerRecord<String, String>(topicName, dp3.toJsonString()));
                                        //ObjectMapper objectMapper = new ObjectMapper();
                                        //Datapoint dp2 = objectMapper.readValue(dp.toJsonString(), Datapoint.class);

                                       System.out.println(dp.toJsonString());

                                        //ObjectMapper mapper = new ObjectMapper();
                                        //JsonNode node = mapper.readTree(dp2.getValues());
                                        //String phoneType = node.get("price").asText();
                                        //String cat = node.get("StockID").asText();


                                        nOfMessages++;
                                    }
                                }

                            }
                        }
                    }
                }
            }

        System.out.println(line);
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }
*/


}