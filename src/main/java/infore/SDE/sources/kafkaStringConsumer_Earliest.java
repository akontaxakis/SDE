package infore.SDE.sources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class kafkaStringConsumer_Earliest {

    private FlinkKafkaConsumer<String> fc;



    public kafkaStringConsumer_Earliest(String server, String topic) {
        Properties properties = new Properties();


        properties.setProperty("bootstrap.servers", server);
        properties.setProperty("group.id", "test");

        fc = (FlinkKafkaConsumer<String>) new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties).setStartFromEarliest();

    }

    public void cancel() {

        fc.cancel();

    }

    public FlinkKafkaConsumer<String> getFc() {
        return fc;
    }

    public void setFc(FlinkKafkaConsumer<String> fc) {
        this.fc = fc;
    }
}
