import message.Datapoint;
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
        String topicData = "test";
        // create instance for properties to access producer configs
        Properties props = new Properties();
        //Assign localhost id KafkaAPI
        props.put("bootstrap.servers", "45.10.26.123:19092,45.10.26.123:29092,45.10.26.123:39092");

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

    Datapoint dp = new Datapoint();
    int i = 0;
    int min = 10;
    int max = 100;
    Random r = new Random();
    while (i < 10000) {

        int randomValue = 10 + r.nextInt(max - min);
        //a unique key per DataSet
        dp.setStreamID("INTEL");
        dp.setValues(Integer.toString(randomValue));
        dp.setDataSetkey("FINANCIAL_USECASE");
        System.out.println(dp.keyToKafka() + " " + dp.ValueToKafka());
        producer.send(new ProducerRecord<String, String>(topicData,dp.keyToKafka(), dp.ValueToKafka()));
        i++;

    }
    producer.close();
}
}
