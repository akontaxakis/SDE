import com.fasterxml.jackson.databind.ObjectMapper;
import messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Properties;

public class RequestFIN {

    public static void main(String[] args) throws Exception {

        String topicRequests = "testRequest";

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
        //Parameters are based on each specific query or Synopsis creation check .excel file for details
        //if less than the correct number of parameters are send then the default values will be used.
        String[] parameters = {"ForexXOFNoExpiry", "2"};
        String[] parameters2 = {"0.95", "2"};
        String key="FOREX";

        ArrayList<Request> rqs = new ArrayList<Request>();

        rqs.add((new Request(key, 1, 4, 1140, "FOREX", parameters2, 4)));
        //rqs.add((new Request("FINANCIAL_USECASE", 1, 6, 116, "FOREX", parameters, 1)));

        //there is a high change that the classes keyToKafka and ValueToKafka may change but that shouldn't affect the client code much.
        for( Request rq: rqs ) {
            rq.setRequestID(3);
            // producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.ValueToKafka()));
            System.out.println(rq.toJsonString());
            String k = rq.toJsonString();
            ObjectMapper objectMapper = new ObjectMapper();

            // byte[] jsonData = json.toString().getBytes();
            Request request = objectMapper.readValue(k, Request.class);
            System.out.println(request.toString());
        }
        producer.close();

    }
}