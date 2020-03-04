package infore.SDE.synopses;

import java.util.HashMap;

import org.apache.commons.math3.complex.Complex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.streaminer.stream.quantile.GKQuantiles;
import com.clearspring.analytics.stream.frequency.CountMinSketch;
import java.io.Serializable;
import infore.SDE.sketches.TimeSeries.windowDFT;
import org.streaminer.stream.cardinality.HyperLogLog;
import org.streaminer.stream.frequency.AMSSketch;
public class DeliverableMSynopsis implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private  HashMap<String, HyperLogLog> Synopses;

	public DeliverableMSynopsis() {
		
		Synopses = new HashMap<String, HyperLogLog>();
	
	}

	@SuppressWarnings("deprecation")
	public void add(String j,double k) {
		
		HyperLogLog DFT = Synopses.get(j);
		if(DFT == null) {
		//DFT = new windowDFT(5,15,8,1);
		 //DFT =	new AMSSketch(5,20);
		//DFT = new	CountMinSketch(0.0002,0.99,4);
			DFT = new HyperLogLog(0.02);
		}
		DFT.offer(k);
		
		Synopses.put(j, DFT);
		
		//return new Tuple2<>(DFT.keyHash((double) k),DFT.getNormalizedFourierCoefficients());
		
	}

	public Object estimate(Object k) {

		return null;
	}



}

