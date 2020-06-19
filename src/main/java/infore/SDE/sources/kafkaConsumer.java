package infore.SDE.sources;

import java.util.Properties;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
public class kafkaConsumer {
    
 private FlinkKafkaConsumer<ObjectNode> fc;
	


	public kafkaConsumer(String server, String topic) {
	Properties properties = new Properties();
	
	
	properties.setProperty("bootstrap.servers", server); 
	properties.setProperty("group.id", "test");
	//.setStartFromEarliest()
	fc = (FlinkKafkaConsumer<ObjectNode>) new FlinkKafkaConsumer<>(topic, new JSONKeyValueDeserializationSchema(false), properties).setStartFromEarliest();
	
	}

	public void cancel() {
		
		fc.cancel();
	
	}

public FlinkKafkaConsumer<ObjectNode> getFc() {
		return fc;
		}

public void setFc(FlinkKafkaConsumer<ObjectNode> fc) {
		this.fc = fc;
		}
		}
