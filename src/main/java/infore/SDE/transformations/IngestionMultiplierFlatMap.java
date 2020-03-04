package infore.SDE.transformations;

import java.util.Random;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class IngestionMultiplierFlatMap implements  FlatMapFunction<ObjectNode, Tuple2<String, String>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	int feedMultiplier;
	
	double start = 0.01;
	double end = 0.09;
	
	public IngestionMultiplierFlatMap(int featMultiplier ) {
		feedMultiplier = featMultiplier;
	}
	@Override
	public void flatMap(ObjectNode value, Collector<Tuple2<String, String>> out) throws Exception {
		double random;
		double result;
		String[] v =  value.get("value").toString().replace("\"", "").split(";");

		out.collect(new Tuple2<>(value.get("key").toString().replace("\"", ""),v[0]));
    
	for(int i = 0; i<feedMultiplier; i++) {
		random = new Random().nextDouble();
		result = start + (random * (end - start)) + Double.parseDouble(v[0]);
		out.collect(new Tuple2<>(value.get("key").toString().replace("\"", ""),Double.toString(result)));
		
	}		
	}
			
		
	}


