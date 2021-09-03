package infore.SDE.sources;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class kafkaProducer2 {
	
FlinkKafkaProducer<Tuple2< String, String>> myProducer;
	
	
	public kafkaProducer2(String brokerlist, String outputTopic) {
		
		myProducer = new FlinkKafkaProducer<>(
					brokerlist,            // broker list
			        outputTopic,                  // target topic
			        (SerializationSchema<Tuple2< String, String>>)new  AverageSerializer2()); 
			        //new SimpleStringSchema());
		
	}
	
	public SinkFunction<Tuple2<String, String>> getProducer(){
		return myProducer;
	}


	class AverageSerializer2 implements SerializationSchema<Tuple2< String, String>> {
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

		public String getTargetTopic(Tuple2<String, String> element) {
			// use always the default topic
			return null;
		}

		@Override
		public byte[] serialize(Tuple2<String, String> element) {

			return ("\""+element.getField(0)+ ","+element.getField(1) +"\"").getBytes();
		}

}
 }
