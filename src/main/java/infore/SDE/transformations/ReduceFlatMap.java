package infore.SDE.transformations;


import java.util.Arrays;
import java.util.HashMap;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.ReduceFunctions.ReduceFunction;
import infore.SDE.ReduceFunctions.SimpleAvgFunction;
import infore.SDE.ReduceFunctions.SimpleMaxFunction;
import infore.SDE.ReduceFunctions.SimpleORFunction;
import infore.SDE.ReduceFunctions.SimpleSumFunction;
import infore.SDE.ReduceFunctions.SpecialReduce;
import infore.SDE.messages.Estimation;


public class ReduceFlatMap extends RichFlatMapFunction<Estimation, Estimation> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private HashMap<String, ReduceFunction> rf = new HashMap<String, ReduceFunction>();
	
	@Override
	public void flatMap(Estimation value, Collector<Estimation> out)
			throws Exception {
		System.out.println(value.toString());
		ReduceFunction t_rf = rf.get(""+value.getEstimationkey());
		int id = value.getSynopsisID();
	    if(t_rf == null) {
	    	//MAX
	    	if(id == 11) {
			 t_rf = new SimpleMaxFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
			 t_rf.add(value);
			 rf.put(""+value.getEstimationkey(), t_rf);
	    	}
	    	//AVG
	    	else if(id == 15) {
				 t_rf = new SimpleAvgFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getEstimationkey(), t_rf);
		    }
	    	//SUM
	    	else if(id == 1 || id == 3 || id == 8 || id == 9 || id == 7  ) {
				 t_rf = new SimpleSumFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getEstimationkey(), t_rf);
		    }
	    	//OR
	    	else if(id == 2) {
				 t_rf = new SimpleORFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getEstimationkey(), t_rf);
		    }
	    	else if(id == 4 || id == 6) {
				//System.out.println("START");
				 t_rf = new SpecialReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getEstimationkey(), t_rf);
		    }
	    }else {
		    	if(t_rf.add(value)) {
					//System.out.println("CAL");
		    		Object output = t_rf.reduce();
					if(output !=null) {
						value.setEstimation(output);
						rf.remove("" + value.getEstimationkey());
						//System.out.println("SEND");
						out.collect(value);
					}
		    	}	
	      }
		}			
	  }	
