package infore.SDE.sources;

import org.apache.commons.math3.complex.Complex;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import infore.SDE.messages.Estimation;

public class kafkaProducerEstimation {

	FlinkKafkaProducer<Estimation> myProducer;


	public kafkaProducerEstimation(String brokerlist, String outputTopic) {

		myProducer = new FlinkKafkaProducer<>(
				brokerlist,            // broker list
				outputTopic,                  // target topic
				(KeyedSerializationSchema<Estimation>) new EstimationSerializer());
		//new SimpleStringSchema());
		myProducer.setWriteTimestampToKafka(true);

	}

	public SinkFunction<Estimation> getProducer() {
		return myProducer;
	}

}

 class EstimationSerializer implements KeyedSerializationSchema<Estimation> {
    /**
	 *
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public byte[] serializeKey(Estimation element) {
			return (""+element.getUID()).getBytes();
	}

	@Override
	public byte[] serializeValue(Estimation element) {

			return element.toKafka();

	}
	@Override
	public String getTargetTopic(Estimation element) {
		// TODO Auto-generated method stub
		return null;
	}
  }
