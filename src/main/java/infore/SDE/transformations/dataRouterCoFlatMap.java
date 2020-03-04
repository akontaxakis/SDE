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
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;

public class dataRouterCoFlatMap extends RichCoFlatMapFunction<Tuple2<String, String>, Request, Tuple2<String, String>> {

	private static final long serialVersionUID = 1L;
	// SourceID (1-5), StreamID(1-10000), Keys(1-1000),
	
	//UID, parallelism, index, 
	ArrayList<Tuple3<Integer, Integer, Integer>> RegisteredSynopses = new ArrayList<Tuple3<Integer, Integer, Integer>>();
	HashMap<String, ArrayList<String>> keysPerDataPoint = new HashMap<String, ArrayList<String>>();
                                        //UID, SynopsisID, Parameters, Parallelism        
	private transient ValueState<ArrayList<Tuple4<Integer,Integer, String,Integer>>> rs;

	@Override
	public void flatMap1(Tuple2<String, String> value, Collector<Tuple2<String, String>> out) throws Exception {
     
		
		String[] value_tokens = value.f1.split(",");
		
		//Send Data with default Key the StreamID
		value.f0 = value_tokens[0];
		out.collect(value);
		
		if (RegisteredSynopses != null) {

			ArrayList<String> tmp2 = keysPerDataPoint.get(value_tokens[0]);
			if (tmp2 == null)
				tmp2 = new ArrayList<String>();

			if (tmp2.size() != RegisteredSynopses.size()) {
				Iterator<Tuple3<Integer, Integer, Integer>> iter = RegisteredSynopses.iterator();
				int i = 0;
				while (iter.hasNext()) {
					Tuple3<Integer, Integer, Integer> v = iter.next();
					i++;
					if (i > tmp2.size()) {
						if (v.f1 == v.f2)
							v.f1 = 0;
						tmp2.add(value_tokens[0] + "_" + v.f1.toString());
						v.f1++;

					}
				}
			}

			if (tmp2.size() == RegisteredSynopses.size()) {
				Iterator<String> iter = tmp2.iterator();
				while (iter.hasNext()) {
					value.f0 = iter.next();
					out.collect(value);
				}
			}
			keysPerDataPoint.put(value_tokens[0], tmp2);
		}
	}

	@Override
	public void flatMap2(Request rq, Collector<Tuple2<String, String>> out) throws Exception {

		if (rq.getRequestID() % 10 == 1 && rq.getNoOfP() > 1) {

			if (RegisteredSynopses == null) {
				RegisteredSynopses = new ArrayList<Tuple3<Integer, Integer, Integer>>();
			}
			Tuple3<Integer, Integer, Integer> indx = new Tuple3<Integer, Integer, Integer>(rq.getUID(), 0, rq.getNoOfP());
			RegisteredSynopses.add(indx);
			
			if(rs.value() !=null) {
			ArrayList<Tuple4<Integer,Integer, String,Integer>> tmp = rs.value();
			tmp.add(new Tuple4<Integer,Integer, String,Integer>(rq.getUID(),rq.getSynopsisID(),rq.getParam().toString(),rq.getNoOfP()));
			rs.update(tmp);
			System.out.print("update");
			}else {
				ArrayList<Tuple4<Integer,Integer, String,Integer>> tmp = new ArrayList<Tuple4<Integer,Integer, String,Integer>>();
				tmp.add(new Tuple4<Integer,Integer, String,Integer>(rq.getUID(),rq.getSynopsisID(),rq.getParam().toString(),rq.getNoOfP()));
				System.out.print("update");
				rs.update(tmp);
			}
		}		
	}

	public void open(Configuration config) {
		
		ValueStateDescriptor<ArrayList<Tuple4<Integer,Integer, String,Integer>>> descriptor = new ValueStateDescriptor<>("Synopses", // the state
	    TypeInformation.of(new TypeHint<ArrayList<Tuple4<Integer,Integer, String,Integer>>>() {})); // type information
	    descriptor.setQueryable("getSynopses");
		rs = getRuntimeContext().getState(descriptor);
	}

}
