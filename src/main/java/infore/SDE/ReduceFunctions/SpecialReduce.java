package infore.SDE.ReduceFunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import infore.SDE.sketches.TimeSeries.COEF;
import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import Coresets.KmeansPP;
import Coresets.Point;
import Coresets.TreeCoreset;

public class SpecialReduce extends ReduceFunction {

	public SpecialReduce(int nOfP, int count, String[] parameters, int syn) {
		super(nOfP, count, parameters, syn);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		int counter=0;
		String str = " ";
		double correlation;
		if (this.getSynopsisID() == 4) {
			for (Object entry : this.getEstimations()) {
				COEF coefficients0 = (COEF) entry;
				for (Object entry2 : this.getEstimations()) {
					COEF coefficients1 = (COEF) entry2;

					if (coefficients0.getFourierCoefficients() == null || coefficients1.getFourierCoefficients() == null) {
						break;
					}
					if(!((COEF) entry).getStreamKey().startsWith(((COEF) entry2).getStreamKey())) {
						double dist = distance(coefficients0.getFourierCoefficients(), coefficients1.getFourierCoefficients()) / 2;
						correlation = 1 - dist;

						if (correlation > Double.parseDouble(this.getParameters()[0]) && correlation<1) {
							str = str.concat("[" + coefficients0.getStreamKey() + "_" + coefficients1.getStreamKey() + "]");
							counter++;
						}
					}
				}

			}
			if(counter ==0)
			return null;

			return counter;
		}
		if (this.getSynopsisID() == 14) {
			for (Object entry : this.getEstimations()) {
				ArrayList<Integer> c0 = (ArrayList<Integer>) entry;
				for (Object entry2 : this.getEstimations()) {
					ArrayList<Integer> c1 = (ArrayList<Integer>) entry2;
					counter++;
					if(c0.size()==c1.size()) {

						if(c0.size() == 1) {
							c0.add(0);
							c1.add(0);
						}
						double corr = new PearsonsCorrelation().correlation(convertIntegers(c0), convertIntegers(c1));

						if (corr > Double.parseDouble(this.getParameters()[0]))
							count++;
					}else {
						if(c0.size()>c1.size()) {
							for (int i = c1.size(); i < c0.size(); i++) {
								c1.add(0);
							}
						}else {
							for (int i = c0.size(); i < c1.size(); i++) {
								c0.add(0);
							}
						}
						if(c0.size() == 1) {
							c0.add(0);
							c1.add(0);
						}
						double corr = new PearsonsCorrelation().correlation(convertIntegers(c0), convertIntegers(c1));

						if (corr > Double.parseDouble(this.getParameters()[0]))
							count++;

					}
				}

			}
			return counter;
		}if (this.getSynopsisID() == 6) {
			Random random = new Random(1);

			int rc =0;
			Point[] finalCorest = null;
			for (Object entry : this.getEstimations()) {
				Point[] value = (Point[]) entry;
				if(rc == 0){
					rc++;
					finalCorest = value;
				}
				else if (finalCorest.length > 0 || value.length > 0)
					finalCorest = TreeCoreset.unionTreeCoreset(finalCorest, value, Integer.parseInt(parameters[3]), Integer.parseInt(parameters[2]), random);
			}
			Point[] centroids = KmeansPP.applyKmeansPP(finalCorest, 1, -1, Integer.parseInt(parameters[4]), Integer.parseInt(parameters[2]), random);

			//Build string format "[weight x y z ...]"
			StringBuilder sb = new StringBuilder();
			for(Point p : centroids){
				sb.append(p.toString()).append("\n");
			}

			return sb.toString();


		}
		return counter;

	}

	private double distance(Complex[] a, Complex[] b) {
		double distance = 0;
		for (int i = 1; i < a.length; i++) { // The first coefficient is always 0
			distance += Math.pow((a[i].getReal() - b[i].getReal()), 2);
			distance += Math.pow((a[i].getImaginary() - b[i].getImaginary()), 2);
		}
		return distance;
	}

	private static double[] convertIntegers(List<Integer> integers) {
		double[] ret = new double[integers.size()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = integers.get(i);
		}
		return ret;
	}

}