package infore.SDE.transformations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import infore.SDE.messages.Request;

public class OlddataRouterCoFlatMap extends RichCoFlatMapFunction<Tuple2<String, String>, Request, Tuple2<String, String>> {

	private static final long serialVersionUID = 1L;
	// SourceID (1-5), StreamID(1-10000), Keys(1-1000),
	
	//UID, parallelism, index, 
	private ArrayList<Tuple3<Integer, Integer, Integer>> RegisteredSynopses = new ArrayList<Tuple3<Integer, Integer, Integer>>();
	private HashMap<String, ArrayList<String>> keysPerDataPoint = new HashMap<>();
	private HashMap<String,String> Synopses =  new HashMap<>();
                                        //UID, SynopsisID, Parameters, Parallelism        
	private transient ValueState<Tuple1<String>> rs;

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
				for (String s : tmp2) {
					value.f0 = s;
					out.collect(value);
				}
			}
			keysPerDataPoint.put(value_tokens[0], tmp2);
		}
	}

	@Override
	public void flatMap2(Request rq, Collector<Tuple2<String, String>> out) throws Exception {


		if (rq.getRequestID() % 10 == 1) {

			if (rq.getNoOfP() > 1) {

				if (RegisteredSynopses == null) {
					RegisteredSynopses = new ArrayList<>();

				}
				Tuple3<Integer, Integer, Integer> indx = new Tuple3<Integer, Integer, Integer>(rq.getUID(), 0, rq.getNoOfP());
				RegisteredSynopses.add(indx);
			}
			String newS = "[" + rq.getUID() + "," + rq.getSynopsisID() + "," + Arrays.toString(rq.getParam()) + "," + rq.getNoOfP() + "]";

			if (rs.value() == null) {

				Tuple1<String> tmp = new Tuple1<String>(newS);
				Synopses.put(""+rq.getUID(), newS);
				rs.update(tmp);

				//System.out.print("\n StateInit");
			} else {

				Synopses.put("" +rq.getUID(), newS);
				Tuple1<String> tmp = rs.value();
				tmp.f0 = newS.concat(tmp.f0);
				//System.out.print("\n StateUpdate");
				rs.update(tmp);
			}
			}
			else if (rq.getRequestID() % 10 == 2){

			Synopses.remove("" + rq.getUID());

			String tmp="";
			if(Synopses.size()>0) {
				for (String entry : Synopses.values()) {
					tmp = entry.concat(tmp);
				}

				Tuple1<String> tmp2 = new Tuple1<>(tmp);
				rs.update(tmp2);

			}else{
				rs.update(new Tuple1<String>(""));
			}
		}
	}

	public void open(Configuration config) {


		TypeInformation<Tuple1<String>> typeInformation = TypeInformation.of(new TypeHint<Tuple1<String>>() {});


		ValueStateDescriptor<Tuple1<String>> descriptor = new ValueStateDescriptor("Synopses", typeInformation);
	    descriptor.setQueryable("getSynopses");
		rs = getRuntimeContext().getState(descriptor);

	}

}
