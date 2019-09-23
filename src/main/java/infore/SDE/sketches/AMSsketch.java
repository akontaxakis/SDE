package infore.SDE.sketches;
import org.streaminer.stream.frequency.AMSSketch;

public class AMSsketch extends Sketch{
	AMSSketch ams;
	
	
	public AMSsketch(int depth, int buckets) {
		ams = new AMSSketch(depth, buckets);
	}
	
	@Override
	public void add(Object k) {
		ams.add((long)k);
		
	}

	@Override
	public String estimate(Object k) {
		// TODO Auto-generated method stub
		return Long.toString(ams.estimateCount((long)k));
	}

	@Override
	public Sketch merge(Sketch sk) {
		// TODO Auto-generated method stub
		return null;
	}

}
