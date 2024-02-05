import messages.Datapoint;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import messages.Datapoint;

public class SendData{

    public static void main(String[] args) throws Exception{

    //Kafka Configuration
        //Assign topicName to string variable
        String topicName = "data_topic";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id KafkaAPI
        //props.put("bootstrap.servers", "45.10.26.123:19092,45.10.26.123:29092,45.10.26.123:39092");
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


    //Code for Sending DataPoints to the SDE after the configuration of a kafka Producer
    //Datapoint dp = new Datapoint();
    int i = 0;
    int min = 10;
    int max = 100;
    Random r = new Random();
    while (i < 1000) {

        int randomValue = 100 + r.nextInt(max - min);
        int randomkey = 10 + r.nextInt(100);
        String jsonString = "{\"a1\":\""+ randomkey+"\",\"label\":\"" + randomValue + "\"}";
        System.out.println(jsonString);
        Datapoint dp = new Datapoint("Polynomial_Data", "1", jsonString);
        System.out.println(dp.toJsonString());
        //producer.send(new ProducerRecord<String, String>(topicName,dp.toJsonString()));
        i++;

    }
    producer.close();
}
}
