package infore.SDE.Experiments;

import infore.SDE.messages.Datapoint;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;
import java.util.ArrayList;

public class NaiveCoFlatMap implements  FlatMapFunction<Datapoint, Datapoint>{

	/**
	 *
	 */

	private static final long serialVersionUID = 1L;

	private ArrayList<Datapoint> dps = new ArrayList<>();
	private int count = 0;
	private int threshold =70;

	@Override
	public void flatMap(Datapoint value, Collector<Datapoint> out) throws Exception {
        count++;
		dps.add(value);

		if(count>100) {
			count =0;
			ArrayList<Datapoint> result = compare(dps,value);
			if(!result.isEmpty()) {
				for(Datapoint dp: result)
				out.collect(dp);
			}
		}


		}

	private ArrayList<Datapoint> compare(ArrayList<Datapoint> dps, Datapoint value) {
		ArrayList<Datapoint> result = new ArrayList<>();

		for(Datapoint dp: dps){
			if(dp.compare(value)){
				result.add(dp);
			}
	}
		return result;
}


}


