package infore.SDE.sketches;

import org.apache.commons.math3.complex.Complex;
import org.apache.flink.api.java.tuple.Tuple2;

import infore.SDE.sketches.TimeSeries.MyTimeSeries;

public class TimeSeriesSketch extends Sketch{
     MyTimeSeries ts;
  
	
	public TimeSeriesSketch(String StreamId, int bw) {
		ts = new MyTimeSeries(StreamId, bw);
	}
	
	@Override
	public void add(Object k) {
		ts.pushToValues((double)k);
	}

	@Override
	public Object estimate(Object k) {
		return ts.getNormalizedFourierCoefficients();
	}

	@Override
	public Sketch merge(Sketch sk) {
		return null;
	}
    
	public Tuple2<String, Complex[]>  indexedEstimation(Object k) {
		
		return new Tuple2<>(ts.getGridHashKey((double) k),ts.getNormalizedFourierCoefficients());
	}
	
	
	public String COEFtoString() {
		int COEFFICIENTS_TO_USE = 2;
		Complex[] fourierCoefficients = ts.getNormalizedFourierCoefficients();
		String answer = " ";
		for (int m = 1; m < COEFFICIENTS_TO_USE ; m++) {
            answer = answer + fourierCoefficients[m].getReal() + "  ";
        
            answer = answer + fourierCoefficients[m].getImaginary() + "  "; 
            if(Double.parseDouble(ts.getM())>1.4)
            answer = answer + " ERROR// " + ts.getM();
            else
            answer = answer + " // " + ts.getM();
        }
		
	return answer;	
	}
	

}
