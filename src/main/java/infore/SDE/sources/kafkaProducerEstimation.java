package infore.SDE.sources;

import com.fasterxml.jackson.core.JsonProcessingException;
import infore.SDE.messages.Request;
import lib.WDFT.PAIR;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import infore.SDE.messages.Estimation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;

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
		//System.out.println("MESSAGE TO WRITE -> " + element.getEstimationkey() +"_"+element.getRequestID());
		if(element.getRequestID()==7){
			Request rq = new Request(element);
			try {
				return rq.toKafkaJson();
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		} else if (element.getSynopsisID()==29) {
			if(element.getEstimation()!=null) {
				String k = toKafka(element);
				return k.getBytes();
			}
		}else {
			try {
				return element.toKafkaJson();

			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
			return element.toKafka();
		}
		return "NULL_ESTIMATION".getBytes();
	}
	@Override
	public String getTargetTopic(Estimation element) {
		// TODO Auto-generated method stub
		return null;
	}

	 private String toKafka(Estimation e) {

		 String par = Arrays.toString(e.getParam()).replace(",", ";");
		 par = par.substring(1, par.length()-1).replaceAll("\\s+","");
		 String estimation_string = ((HashMap<String, LinkedList<PAIR>>)e.getEstimation()).toString();
		 return ("\""+e.getEstimationkey()+",Request:"+e.getRequestID()+","+estimation_string+","+par+","+e.getNoOfP()+"\"");
		 //return ("\"KEY:_"+estimationkey+" SYNOPSIS:_"+SynopsisID+" ESTIMATION:_"+estimation+"_\"").getBytes();
	 }

  }
