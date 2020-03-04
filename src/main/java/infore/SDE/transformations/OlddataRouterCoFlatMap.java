package infore.SDE.transformations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;

public class OlddataRouterCoFlatMap extends RichCoFlatMapFunction<Tuple2<String, String>, Request, Tuple2<String, String>> {	
    /*
     *
     *   
	 */	
	private static final long serialVersionUID = 1L;	
	 //SourceID (1-5), StreamID(1-10000), Keys(1-1000),	
	 HashMap<String, ArrayList<Tuple3<Integer,Integer,Integer>>> RegisteredSynopsis = new HashMap<String, ArrayList<Tuple3<Integer,Integer,Integer>>>();
	 HashMap<String, ArrayList<String>> keysPerDataPoint = new HashMap<String, ArrayList<String>>();

	 
	 private transient ValueState<Tuple2<String, String>> rs;
	 
		@Override
		public void flatMap1(Tuple2<String, String> value, Collector<Tuple2<String, String>> out)
		 throws Exception {
			
			out.collect(value);
			ArrayList<Tuple3<Integer,Integer,Integer>> tmp = RegisteredSynopsis.get(value.f0);
			String[] value_tokens = value.f1.split(",");
			if(tmp != null) {     
				
				ArrayList<String> tmp2 = keysPerDataPoint.get(value_tokens[0]);
				if(tmp2 == null)
				   tmp2 = new ArrayList<String>();
				
				if( tmp2.size() != tmp.size()) {
					Iterator<Tuple3<Integer,Integer,Integer>> iter = tmp.iterator();
				int i = 0;
				while (iter.hasNext()) {					
					Tuple3<Integer,Integer,Integer> v = iter.next();
					i++;
					if(i > tmp2.size()) {
						if(v.f1 == v.f2)
						v.f1 = 0;			
						tmp2.add(v.f0.toString() + "_" + v.f1.toString());
						v.f1++; 
			        
					}
			      }		
				}	
					
				if( tmp2.size() == tmp.size()) {
					Iterator<String> iter = tmp2.iterator();
					while (iter.hasNext()) {					
				         value.f0 = iter.next();
				         out.collect(value);
				      }			
				 }
				 keysPerDataPoint.put(value_tokens[0],tmp2);
			}	
		}		
		@Override
		public void flatMap2(Request rq, Collector<Tuple2<String, String>> out)
				throws Exception {
		/*
			if(rq.getRequestID()%10 == 1) {
				Tuple2<String, String> currentList = rs.value();
				currentList.f0 += ","+rq.getSynopsisID();
				currentList.f1 += ","+rq.getParam();
		        rs.update(currentList);
			}*/
			
			
			if(rq.getRequestID()%10 == 1 && rq.getNoOfP() > 1) {
			
			ArrayList<Tuple3<Integer,Integer,Integer>> tmp = RegisteredSynopsis.get(rq.getKey());
			Tuple3<Integer,Integer,Integer>  indx;
				if(tmp == null){
					  tmp = new ArrayList<Tuple3<Integer,Integer,Integer>>();
				}				
				indx = new Tuple3<Integer,Integer,Integer>(rq.getUID(),0,rq.getNoOfP());
				tmp.add(indx);
				RegisteredSynopsis.put(rq.getKey(), tmp);
					  }
				}
		
	    public void open(Configuration config) {
	        ValueStateDescriptor<Tuple2<String, String>> descriptor =
	                new ValueStateDescriptor<>(
	                        "average", // the state name
	                        TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})); // type information
	        descriptor.setQueryable("query-name");
	        rs = getRuntimeContext().getState(descriptor);
	    }
		
		
		
		
		}
