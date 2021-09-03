package infore.SDE.producersForTesting;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import infore.SDE.messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.net.URL;
import java.util.*;

public class sendAISTest {
    public void run(String kafkaDataInputTopic, String KafkaRequestTopic, int p){
        try {
            SendaddRequest(KafkaRequestTopic,p);
            sendAISData(kafkaDataInputTopic);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void SendaddRequest(String kafkaRequestTopic, int p) throws JsonProcessingException {

        String topicRequests = kafkaRequestTopic;
        String[] parameters5 = {"shipid","shipid","t","60","500","1","1"};;



        Properties props = new Properties();
        //Assign localhost id
        //props.put("bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
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



        Request rq = new Request("AIS_useCase", 5, 12, 1110, "INTEL", parameters5, p);


        producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));
        System.out.println(rq.toJsonString());
        producer.close();
    }

    private static void sendAISData(String kafkaDataInputTopic) throws IOException {

        InputStream in = sendAISTest.class.getResourceAsStream("/data/ais2.csv");
        BufferedReader br = new BufferedReader(new InputStreamReader(in));


        String topicName = kafkaDataInputTopic;

        String line = "";
        int jk, i, j;
        int nOfMessages = 0;
        int Gcount = 0;


        HashMap<String, Integer> mp = new HashMap<String, Integer>();
        HashMap<String, ArrayList<String>> keys = new HashMap<>();
        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
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


        HashMap<String, Integer> dataStatistics = new HashMap<>();
        //int multi = 1;
        int multi = 28;
        int nkeys = multi * 361;
        line = br.readLine();
        line = br.readLine();
        while (line != null) {

            String[] words = line.split(",");
            String AISkey = words[1];
            if (keys.get(AISkey) == null) {
                ArrayList<String> tmpArray = new ArrayList<>();
                for (int k = 0; k < multi; k++) {
                    tmpArray.add(AISkey + k);
                }
                keys.put(AISkey, tmpArray);
            }
            ArrayList<String> t_keys = keys.get(AISkey);
            for (String key : t_keys){
                AISkey = key;
                String jsonString;
                if(words.length>5) {
                    jsonString = "{\"t\":\"" + words[0] + "\",\"shipid\":\"" + AISkey + "\",\"lon\":\"" + words[2] + "\",\"lat\":\"" + words[3] + "\",\"course\":\"" + words[4] + "\",\"speed\":\"" + words[6] + "\"} ";
                    keepStatistics(dataStatistics, AISkey);

                    //data string
                    //System.out.println(jsonString);
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(jsonString);
                    //String phoneType = node.get("phonetype").asText();
                    //String cat = node.get("cat").asText();


                    Datapoint dp = new Datapoint("AIS_useCase", AISkey, node);
                    //Datapoint dp =  new Datapoint("Forex", stock2,words[0]+" "+words[1]+","+words[2]);
                    producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));

                    //SDE data string
                    System.out.println(dp.toJsonString());


                    nOfMessages++;
                    //System.out.println(nOfMessages + " id ->" + stock2);
                    //System.out.println("Message sent successfully -> " + nOfMessages);
                }
            }
            line = br.readLine();
        }
        for (Map.Entry me : dataStatistics.entrySet()) {
            System.out.println("Key: "+me.getKey() + " & Value: " + me.getValue());
        }


        producer.close();
    }
    private static void sendAISData2(String kafkaDataInputTopic) throws IOException {
        URL url = sendAISTest.class.getResource("data/ais2.csv");
        File file = new File(url.getPath());
        String topicName = kafkaDataInputTopic;

        String line = "";
        int nOfMessages = 0;
        int Gcount = 0;


        Properties props = new Properties();
        props.put("bootstrap.servers", "clu02.softnet.tuc.gr:6667,clu03.softnet.tuc.gr:6667,clu04.softnet.tuc.gr:6667,clu06.softnet.tuc.gr:6667");
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

        BufferedReader br = new BufferedReader(new FileReader(file));
        HashMap<String, Integer> dataStatistics = new HashMap<>();
        ArrayList<String> keys = new ArrayList<>();
        int Tkeys = 10;
        int nkeys=0;
        int i=0;
        line = br.readLine();
        line = br.readLine();
        while (line != null) {
            String[] words = line.split(",");
            String AISkey = words[1];

            if(Tkeys>=nkeys){
                if (dataStatistics.get(AISkey) == null) {
                    keys.add(AISkey);
                    nkeys++;
                }
            }
            if(i>=keys.size()){
                i=0;
            }
            AISkey=keys.get(i);
            i++;
            keepStatistics(dataStatistics, AISkey);
            String jsonString;
            if(words.length>5) {
                jsonString = "{\"t\":\"" + words[0] + "\",\"shipid\":\"" + AISkey + "\",\"lon\":\"" + words[2] + "\",\"lat\":\"" + words[3] + "\",\"course\":\"" + words[4] + "\",\"speed\":\"" + words[6] + "\"} ";


                //data string
                //System.out.println(jsonString);
                ObjectMapper mapper = new ObjectMapper();
                JsonNode node = mapper.readTree(jsonString);
                //String phoneType = node.get("phonetype").asText();
                //String cat = node.get("cat").asText();


                Datapoint dp = new Datapoint("AIS_useCase", AISkey, node);
                //Datapoint dp =  new Datapoint("Forex", stock2,words[0]+" "+words[1]+","+words[2]);
                producer.send(new ProducerRecord<String, String>(topicName, dp.toJsonString()));

                //SDE data string
                // System.out.println(dp.toJsonString());


                nOfMessages++;
                //System.out.println(nOfMessages + " id ->" + stock2);
                //System.out.println("Message sent successfully -> " + nOfMessages);
            }

            line = br.readLine();
        }
        for (Map.Entry me : dataStatistics.entrySet()) {
            System.out.println("Key: "+me.getKey() + " & Value: " + me.getValue());
        }


        producer.close();
    }
    private static void keepStatistics(HashMap<String, Integer> dataStatistics, String AISkey) {
        if (dataStatistics.get(AISkey) == null)
            dataStatistics.put(AISkey, 1);
        else {
            Integer tmp = dataStatistics.get(AISkey);
            tmp++;
            dataStatistics.put(AISkey, tmp);
        }
    }
}
