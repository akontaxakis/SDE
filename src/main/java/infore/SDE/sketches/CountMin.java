package infore.SDE.sketches;

import com.clearspring.analytics.stream.frequency.CountMinSketch;

public class CountMin extends Sketch{

	private CountMinSketch cm;

	public CountMin(double eps, double confidence, int seed) {
	 cm = new CountMinSketch(eps,confidence,seed);
	}
	 
	@Override
	public void add(Object k) {
		cm.add(new Double((double) k).longValue(), 1);	
	}

	@Override
	public Object estimate(Object k) {
		return Long.toString(cm.estimateCount(new Long((int)k)));
		
	}

	@Override
	public Sketch merge(Sketch sk) {
		return sk;
		
	}
	
	
	
	
}
