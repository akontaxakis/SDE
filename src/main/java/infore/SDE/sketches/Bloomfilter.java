package infore.SDE.sketches;

import com.clearspring.analytics.stream.membership.BloomFilter;

public class Bloomfilter extends Sketch{
 private BloomFilter bm;
 
 public Bloomfilter(int numElements, double maxFalsePosProbability) {
	 bm = new BloomFilter( numElements, maxFalsePosProbability);
	 
 }
	
	
	
	@Override
	public void add(Object k) {
		bm.add(Double.toString((double)k));
		
	}

	@Override
	public String estimate(Object k) {
		 if(bm.isPresent((Double.toString((double)k))))
		return "1";
		return "0";			 
		
	}

	@Override
	public Sketch merge(Sketch sk) {
		// TODO Auto-generated method stub
		return sk;
	}
 
}
