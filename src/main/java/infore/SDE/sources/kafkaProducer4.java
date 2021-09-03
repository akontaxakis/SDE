package infore.SDE.sources;


import org.apache.commons.math3.complex.Complex;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

public class kafkaProducer4 {

	FlinkKafkaProducer<Tuple4<String, String, Object, String>> myProducer;


	public kafkaProducer4(String brokerlist, String outputTopic) {

		myProducer = new FlinkKafkaProducer<>(
				brokerlist,            // broker list
				outputTopic,                  // target topic
				(KeyedSerializationSchema<Tuple4<String, String, Object, String>>) new AverageSerializer());
		//new SimpleStringSchema());

	}

	public SinkFunction<Tuple4<String, String, Object, String>> getProducer() {
		return myProducer;
	}

	class AverageSerializer implements KeyedSerializationSchema<Tuple4<String, String, Object, String>> {
		/**
		 *
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public byte[] serializeKey(Tuple4<String, String, Object, String> element) {
			// TODO Auto-generated method stub
			return ("\"" + element.getField(0) + "," + element.getField(1) + "\"").getBytes();
		}

		@Override
		public byte[] serializeValue(Tuple4<String, String, Object, String> element) {
			// TODO Auto-generated method stub
			Complex[] k = (Complex[]) element.getField(2);
			return ("\"" + k[0].toString() + "," + k[1].toString() + "\"").getBytes();
		}

		@Override
		public String getTargetTopic(Tuple4<String, String, Object, String> element) {
			// TODO Auto-generated method stub
			return null;
		}
	}
}