import message.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class TestALLadd {

    public static void main(String[] args) throws Exception {

        //Kafka Producer Configuration
        //Assign topicName to string variable
        String topicRequests = "Requests";
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


         /*Synopsis IDs ATM
         CountMin 1
         BloomFilter 2
         AMS 3
         DFT 4
         LSH 5
         Coresets 6
         Hyperloglog 7
         StickySampling 8
         lossyCounting 9
         ChainSampler 10
         GKsynopsis 11 */

         /*RequestIDs ATM
          1 -> add
          2 -> remove
          3 -> estimate
          4 -> continueEstimation
          */

        //Parameters are based on each specific query or Synopsis creation check .excel file for details
        //if less than the correct number of parameters are send then the default values will be used.
        String[] parameters = {"ForexXOFNoExpiry", "2"};
        String[] parameters2 = {"0.95", "2"};
        String[] parameters3 = {"0.95", "2", "5", "10", "2"};
        String key="Forex";

        //uID should be unique per request
        ArrayList<Request> rqs = new ArrayList<Request>();
        //Request rq1 = new Request(key, 1, 1, 111, "INTEL", parameters, 1);
        //rqs.add(( new Request(key, 1, 1, 1110, "FOREX", parameters, 4)));
        //rqs.add(( new Request(key, 1, 2, 1120, "FOREX", parameters, 4)));
        //rqs.add((new Request(key, 1, 3, 1130, "FOREX", parameters, 4)));


        rqs.add((new Request(key, 1, 4, 1140, "Paris", parameters2, 4)));
        //rqs.add((new Request(key, 1, 6, 1160, "Paris", parameters3, 4)));

        // rqs.add((new Request(key, 1, 7, 1170, "FOREX", parameters, 4)));
        //rqs.add((new Request(key, 1, 8, 1180, "FOREX", parameters, 4)));
       // rqs.add((new Request(key, 1, 9, 1190, "FOREX", parameters, 4)));
       // rqs.add((new Request(key, 1, 10, 1110, "FOREX", parameters, 4)));
       // rqs.add((new Request(key, 1, 11, 11110, "FOREX", parameters, 4)));


        //there is a high change that the classes keyToKafka and ValueToKafka may change but that shouldn't affect the client code much.
        for( Request rq: rqs ) {
            rq.setRequestID(3);
            producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.ValueToKafka()));

        }
        producer.close();

    }
}