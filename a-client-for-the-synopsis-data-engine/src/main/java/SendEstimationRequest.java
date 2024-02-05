import messages.Request;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class SendEstimationRequest {
    public static void main(String[] args) throws Exception {


        //Kafka Producer Configuration
        //Assign topicName to string variable
        String topicRequests = "testRequest5";
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

        String[] parameters = {"INTEL"};
        Request rq = new Request("FINANCIAL_USECASE", 3, 1, 111, "INTEL", parameters, 1);

        producer.send(new ProducerRecord<String, String>(topicRequests, rq.keyToKafka(), rq.toJsonString()));
        producer.close();

    }
}
