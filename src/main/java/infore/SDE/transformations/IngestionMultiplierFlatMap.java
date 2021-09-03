package infore.SDE.transformations;

import java.util.Random;

import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

public class IngestionMultiplierFlatMap implements  FlatMapFunction<Datapoint, Datapoint>{

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
	public void flatMap(Datapoint value, Collector<Datapoint> out) throws Exception {
		for(int i = 0; i<feedMultiplier; i++) {

			out.collect(value);

		}
	}


}


