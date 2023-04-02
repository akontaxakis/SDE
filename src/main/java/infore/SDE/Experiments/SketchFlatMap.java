package infore.SDE.Experiments;

import infore.SDE.messages.Datapoint;
import infore.SDE.synopses.RadiusSketch;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class SketchFlatMap implements  FlatMapFunction<Datapoint, Datapoint>{

	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private ArrayList<Datapoint> dps = new ArrayList<>();
	private int count = 0;
	private RadiusSketch RS = null;

	@Override
	public void flatMap(Datapoint value, Collector<Datapoint> out) throws Exception {
        count++;
		if(RS == null){
			String s =  value.getDataSetkey().substring(0,value.getDataSetkey().length()-1);
			String s1 =  s.substring(s.length()-2,s.length()-1);
			if(s1 =="1" || s1=="2"){
				s = s.substring(0,s.length()-1);
			}

			RS = new RadiusSketch(1110,"StockID","price","Partitioner",s,null);
		}
		Datapoint Sketch = RS.getSketch(value);
		dps.add(Sketch);
		if(count>100) {
			ArrayList<Datapoint> result = compare(dps,Sketch);
			if(!result.isEmpty())
				out.collect(value);
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


