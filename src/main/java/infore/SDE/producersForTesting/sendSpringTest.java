package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class sendSpringTest {

    public void run(String kafkaBroker,String kafkaDataInputTopic, String kafkaRequestInputTopic) {
        try {
            SendaddRequest(kafkaBroker,kafkaRequestInputTopic);
            sendFINData(kafkaBroker,kafkaDataInputTopic);
            TimeUnit.SECONDS.sleep(20);
            SendEstimateRequest(kafkaBroker,kafkaRequestInputTopic);

        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    private static void sendFINData(String kafkaBroker, String kafkaDataInputTopic) throws IOException {

        InputStream in = sendAISTest.class.getResourceAsStream("/data/Sum.txt");
        BufferedReader br = new BufferedReader(new InputStreamReader(in));


        String topicName = kafkaDataInputTopic;

        String line = "";
        int jk, i, j;
        int nOfMessages = 0;
        int Gcount = 0;


        HashMap<String, Integer> mp = new HashMap<String, Integer>();
        HashMap<String, ArrayList<String>> keys = new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
        //props.put("bootstrap.servers","localhost:9092");
        //props.put("bootstrap.servers", "45.10.26.123:19092,45.10.26.123:29092,45.10.26.123:39092");

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

        int multi = 1;
        line = br.readLine();
        while (line != null) {
            String[] words = line.split(",");
            String AISkey = words[1];

           for (int k = 0; k < multi; k++) {

                String jsonString;
                if(words.length>1) {
                    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
                    LocalDateTime now = LocalDateTime.now();
                    jsonString = "{\"stockid\":\"" + AISkey + "\",\"value\":\"" +  words[2] + "\",\"time\":\"" +dtf.format(now) + "\"} ";

                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(jsonString);

                    Datapoint dp = new Datapoint("Spring_useCase", AISkey, node);
                    //Datapoint dp =  new Datapoint("Forex", stock2,words[0]+" "+words[1]+","+words[2]);
                    producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));

                    //SDE data string
                   // System.out.println(dp.toJsonString());

                }
            }
            line = br.readLine();
        }
        nOfMessages++;
        System.out.println("Message sent successfully -> " + nOfMessages);
        producer.close();
    }


    private static void SendaddRequest(String kafkaBroker,String topic) throws JsonProcessingException {
        System.out.println("Let's Send Add Request");
        String topicRequests = topic;
        String[] parameters5 = {"stockid", "value","Queryable", "time", "1","5", "20","8"};
        Properties props = new Properties();
        //Assign localhost id
        props.put("bootstrap.servers", kafkaBroker);
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


        Request rq = new Request("Spring_useCase", 1, 4, 1110, "INTEL", parameters5, 4);
        System.out.println(rq.toJsonString());
        producer.send(new ProducerRecord<String, String>(topicRequests, rq.toJsonString()));

        System.out.println("Add Request is OK");
        producer.close();
    }

    private static void SendEstimateRequest(String kafkaBroker,String topic) throws JsonProcessingException {
        System.out.println("Let's Send Estimate Request");
        String topicRequests = topic;
        String[] parameters = {"0.8"};// Th, K, Query_ID,list_of_sims,T
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaBroker);
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


        Request rq = new Request("Spring_useCase", 3, 4, 1110, "INTEL", parameters, 4);

        producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));
        System.out.println(rq.toJsonString());
        System.out.println("Estimate Request is OK");
        producer.close();
    }

}
