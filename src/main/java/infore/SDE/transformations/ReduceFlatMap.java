package infore.SDE.transformations;


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
		//System.out.println(value.toString());
		ReduceFunction t_rf = rf.get(""+value.getUID());
	    if(t_rf == null) {
	    	//MAX
	    	if(value.getRequestID()/10 == 1) {
			 t_rf = new SimpleMaxFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
			 t_rf.add(value);
			 rf.put(""+value.getUID(), t_rf);
	    	}
	    	//AVG
	    	else if(value.getRequestID()/10 == 2) {
				 t_rf = new SimpleAvgFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getUID(), t_rf);
		    }
	    	//SUM
	    	else if(value.getRequestID()/10 == 3) {
				 t_rf = new SimpleSumFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getUID(), t_rf);
		    }
	    	//OR
	    	else if(value.getRequestID()/10 == 4) {
				 t_rf = new SimpleORFunction(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getUID(), t_rf);
		    }
	    	else if(value.getRequestID()/10 == 5) {
				 t_rf = new SpecialReduce(value.getNoOfP(), 0, value.getParam(), value.getSynopsisID());
				 t_rf.add(value);
				 rf.put(""+value.getUID(), t_rf);
		    }
	    	    	
	    }else {
	    	   	
		    	if(t_rf.add(value)) {
		    		value.setEstimation(t_rf.reduce());
		    		rf.remove(""+value.getUID());
		    		out.collect(value);
		    	}	
	      }
		}			
	  }	
