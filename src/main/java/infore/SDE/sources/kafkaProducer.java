package infore.SDE.sources;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class kafkaProducer {

	FlinkKafkaProducer<Tuple2<Integer, Object>> myProducer;
	
	

	public kafkaProducer(String brokerlist, String outputTopic) {
		
		myProducer = new FlinkKafkaProducer<>(
					brokerlist,            // broker list
			        outputTopic,                  // target topic
			        (SerializationSchema<Tuple2<Integer, Object>>)new  AverageSerializer()); 
			        //new SimpleStringSchema()); 
		myProducer.setWriteTimestampToKafka(true);
		
	}
	
	public FlinkKafkaProducer<Tuple2<Integer, Object>> getProducer(){
		return myProducer;
	}
	
}


 class AverageSerializer implements SerializationSchema<Tuple2<Integer, Object>> {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
/*
	@Override
    public byte[] serializeKey(Tuple2 element) {
      return ("\"" + element.getField(0).toString() + "\"").getBytes();
    }

    @Override
    public byte[] serializeValue(Tuple2 element) {
      String value = element.getField(1).toString();
      return value.getBytes();
    }
*/
  
    public String getTargetTopic(Tuple2<Integer, Object> element) {
      // use always the default topic
      return null;
    }

	@Override
	public byte[] serialize(Tuple2<Integer, Object> element) {
		
		return ("\""+element.getField(0).toString()+ ","+element.getField(1).toString() +"\"").getBytes();
	}
  }