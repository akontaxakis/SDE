package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class sendTopKTest {

    public void run(String kafkaDataInputTopic,String kafkaRequestInputTopic) {
        try {
            SendaddRequest(kafkaRequestInputTopic);
            //sendSIMData(kafkaDataInputTopic);
            sendRandomData(kafkaDataInputTopic);
            TimeUnit.SECONDS.sleep(10);
            SendEstimateRequest(kafkaRequestInputTopic);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void SendaddRequest(String topic) throws JsonProcessingException {
        System.out.println("Let's Send Add Request");
        String topicRequests = topic;
        String[] parameters5 = {"simulationId","data","Time","5","4"};
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



        Request rq = new Request("bio_useCase", 1, 13, 1110, "INTEL", parameters5, 4);


        producer.send(new ProducerRecord<String, String>(topicRequests,  rq.toJsonString()));

        System.out.println("Add Request is OK");
        producer.close();
    }

    private static void SendEstimateRequest(String topic) throws JsonProcessingException {
        System.out.println("Let's Send Estimate Request");
        String topicRequests = topic;
        String list_of_sims = "run18_640"+"&run17_30";
        String[] parameters2 = {"5","100"}; // Th, K, Query_ID,list_of_sims,T
        // create instance for properties to access producer configs
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




        Request rq = new Request("bio_useCase", 3, 13, 1110, "INTEL", parameters2, 4);

        producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));

        System.out.println("Estimate Request is OK");
        producer.close();
    }

    private static void sendSIMData(String kafkaDataInputTopic) throws IOException {
        System.out.println("I'm in SEND SIM");
        //String folderPath = "/home/skatara/Simulations100";
        String folderPath = "C:\\Users\\ado.kontax\\Downloads\\simulations";
        //String folderPath = "/Users/herakatara/Documents/ΗΜΜΥ/Bachelor's Thesis/Datasets/Simulations100";
        String line = "";
        int jk;
        int apoptotic = 0;
        int necrotic = 0 ;
        int alive = 0;
        int nOfMessages = 0;
        final File folder = new File(folderPath);
        Properties props = new Properties();
        //props.put("bootstrap.servers","2.84.152.37:9092,2.84.152.37:9094,2.84.152.37:9095");
        //props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        File[] files = folder.listFiles();
        Arrays.sort(files, (f1, f2) -> f1.compareTo(f2));
        for (File fileEntry : files) { //fileEntry == run0, 1, 2...
            if (fileEntry.isDirectory()) {

                String simulation = fileEntry.getName();

                for (File fileEntry2 : fileEntry.listFiles()) {
                    if (fileEntry2.getName().equals("output")) { //fileEntry2 == output folder
                        if (fileEntry2.isDirectory()) {
                            File[] files3 = fileEntry2.listFiles();
                            Arrays.sort(files3, (f3, f4) -> f3.compareTo(f4));
                            for (File fileEntry3 : files3) { //fileEntry3 == cells_00000.txt
                                //For every time we have  different sum of phases
                                String time = null;

                                if (fileEntry3.getName().startsWith("cells")) {


                                    BufferedReader br = new BufferedReader(new FileReader(fileEntry3.getAbsolutePath())); // br1 = values simulation

                                    String k = fileEntry3.getName().toString().replace(".txt", ""); // k == cells_00000
                                    System.out.println(fileEntry3.getAbsolutePath());


                                    line = br.readLine();

                                    while (line != null ) {
                                        if(!line.startsWith("Time")){
                                            String[] words = new String[18];
                                            StringTokenizer tokenizer = new StringTokenizer(line, ";");
                                            //System.out.println(line);
                                            for (jk = 0; jk < 18; jk++) {
                                                words[jk] = tokenizer.nextToken();
                                            }

                                            if ((Integer.parseInt(words[15])== 0) || (Integer.parseInt(words[15]) == 1)){
                                                alive = alive + 1;
                                            }
                                            else if (Integer.parseInt(words[15]) == 100){
                                                apoptotic++;
                                            }
                                            else if ((Integer.parseInt(words[15]) == 101) || (Integer.parseInt(words[15]) == 102)|| (Integer.parseInt(words[15]) == 103)){
                                                necrotic++;
                                            }
                                            else{
                                                ; //do nothing
                                            }
                                            time = words[0];

                                            line = br.readLine();
                                        }else{
                                            line = br.readLine();
                                        }
                                        //After reading all lines
                                    }
                                    if(time != null) {
                                        if (time.startsWith("1440.02"))
                                            time = "1440";
                                        //String jsonString = "{\"Time\":\"" + time + "\",\"data\":\"" + alive + "," + apoptotic + "," + necrotic + "\",\"simulationId\":\"" + simulation + "\"}";
                                        String jsonString = "{\"Time\":\"" + time + "\",\"data\":\"" + alive +"\",\"simulationId\":\"" + simulation + "\"}";


                                        ObjectMapper mapper = new ObjectMapper();
                                        JsonNode node = mapper.readTree(jsonString);

                                        Datapoint1 dp = new Datapoint1("bio_useCase", simulation, node);


                                        producer.send(new ProducerRecord<String, String>(kafkaDataInputTopic, dp.toJsonString()));
                                        nOfMessages++;
                                        apoptotic = 0;
                                        necrotic = 0;
                                        alive = 0;
                                    }



                                } // telos kathe cells

                            }
                        }
                    }
                }
            }

        }
        System.out.println("Message sent successfully ->"+ nOfMessages);
        //flag++;
        nOfMessages =0;
    }

    private static void sendRandomData(String topic) throws IOException {
        String topicName = topic;
        String line = "";
        int jk;
        int apoptotic = 0;
        int necrotic = 0 ;
        int alive = 0;
        int nOfMessages = 0;

        Properties props = new Properties();
        //props.put("bootstrap.servers","2.84.152.37:9092,2.84.152.37:9094,2.84.152.37:9095");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 163840);
        props.put("linger.ms", 0);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        int max = 100;
        int min =1;
        int time =0;

        for(int i=0;i<100000;i++){

            if(i%100==0 && i>0)
                time++;

            alive = (int) (Math.random() * (max - min + 1) + min);

            String simulation = ""+i%100;


            String jsonString = "{\"Time\":\"" + time + "\",\"data\":\"" + alive  + "\",\"simulationId\":\"" + simulation + "\"}";
            //System.out.println(jsonString);

            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(jsonString);

            Datapoint1 dp = new Datapoint1("bio_useCase", simulation, node);

            //System.out.println(dp.toJsonString());
            producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));


            }
    }
}
