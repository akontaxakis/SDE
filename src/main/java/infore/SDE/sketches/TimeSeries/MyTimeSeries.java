package infore.SDE.sketches.TimeSeries;


import org.apache.commons.math3.complex.Complex;


public class MyTimeSeries {

  private String streamId; // The id of this TimeSeries

  private   int basicWindowSize ; // The size of the basic window
  private  int coefficientsToUse; // The number of fourier coefficients to use
  private  int currentPoint;  // Counts how many timepoints have passed in the current window
  private  double sumOfValues; // The sum of all the values during the current window
  private  double sumfOfSquares;
  private String gridHashKey;

  
  
  //synopsis
  private double mean; 
  private double sigma; 


  private Complex[] fourierCoefficients; // The Fourier Coefficients of the TimeSeries
  private Complex[] normalizedFourierCoefficients; // The normalized Fourier Coefficients
 
  public MyTimeSeries(String streamId, int bw) {
      
	this.streamId = streamId;
    basicWindowSize = bw;
    currentPoint = 0;
    coefficientsToUse = 3;
    gridHashKey="1";
    
    fourierCoefficients = new Complex[coefficientsToUse];
    normalizedFourierCoefficients = new Complex[coefficientsToUse];

	  
    for (int m = 0; m < coefficientsToUse; m++) {
      fourierCoefficients[m] = new Complex(0.0, 0.0);
      normalizedFourierCoefficients[m] = new Complex(0.0, 0.0);
    }
  }

  public void pushToValues(double newValue) {
	  currentPoint++;
	if(this.currentPoint<this.basicWindowSize) {  
		this.sumOfValues += newValue;
		this.sumfOfSquares += newValue * newValue;
	    computeNewDFTDigest(newValue);
	    
	}
    if (((double) this.currentPoint) == (this.basicWindowSize)) {
    	
    	this.currentPoint=0;
    	this.mean = 0;
    	this.sigma = 0;
    	
    	this.mean = this.sumOfValues/this.basicWindowSize;
    	this.sigma = Math.sqrt( (this.sumfOfSquares/this.basicWindowSize) - (this.mean * this.mean));
    			
    	  normalizedFourierCoefficients[0] =  new Complex(0.0, 0.0); 
    	 for (int m = 1; m < coefficientsToUse; m++) {
    		 
    		 normalizedFourierCoefficients[m] = fourierCoefficients[m].divide(sigma);
    		 normalizedFourierCoefficients[m] = normalizedFourierCoefficients[m].divide(basicWindowSize);
    		 
    	 }
	        sumOfValues = 0;
	        sumfOfSquares = 0;
	        currentPoint = 0;
        
        for (int k = 0; k < coefficientsToUse; k++) {
        	fourierCoefficients[k] = new Complex(0.0, 0.0);
        }
    }
  }

  private void computeNewDFTDigest(double newValue) {
    for (int m = 0; m < coefficientsToUse; m++) {
    	double exponent = 2 * Math.PI * m * (basicWindowSize - currentPoint) / (basicWindowSize);
    	//double exponent = (2 * Math.PI * m * currentPoint) / (basicWindowSize);
    	Complex exponentToTheE = new Complex(Math.cos(exponent), - Math.sin(exponent));
    	fourierCoefficients[m] = fourierCoefficients[m].add(exponentToTheE.multiply(newValue));
    }
  }
  
  private String keyHash(double threshold) {
	  
	  double epsilon = Math.sqrt(1 - threshold);
	  int hashOffset = (int) Math.ceil(Math.sqrt(2) / (epsilon));
	    String stringKey = "";
	    int tmpIndex;
	    for (int i = 1; i < 2; i++) {
	      tmpIndex = (int) Math.floor(normalizedFourierCoefficients[i + 1].getReal() / epsilon)
	                 + hashOffset;
	      stringKey += tmpIndex;
	      stringKey += ",";
	      tmpIndex =
	          (int) Math.floor(normalizedFourierCoefficients[i + 1].getImaginary() / epsilon)
	          + hashOffset;
	      stringKey += tmpIndex;
	      if (i < 2 - 1) {
	        stringKey += ",";
	      }
	    }
	    return stringKey;
	}

  public String getM() {  
	  return "" + getMagnitude(normalizedFourierCoefficients[1]);
  }  
  
  public String getGridHashKey(double T) {
	  gridHashKey = keyHash(T);
    return gridHashKey;
  }
  
  private double getMagnitude(Complex coefficient) {
	    return Math.sqrt(Math.pow(coefficient.getReal(), 2) + Math.pow(coefficient.getImaginary(), 2));
}
	  	
		
	public Complex[] getNormalizedFourierCoefficients() {
		return normalizedFourierCoefficients;
	}
	

}




