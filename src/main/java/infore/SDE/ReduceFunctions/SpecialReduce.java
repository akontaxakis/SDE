package infore.SDE.ReduceFunctions;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

public class SpecialReduce extends ReduceFunction {

	public SpecialReduce(int nOfP, int count, String[] parameters, int syn) {
		super(nOfP, count, parameters, syn);
		// TODO Auto-generated constructor stub
	}

	@Override
	public Object reduce() {
		int count =0;
		long sum = 0;
		double correlation = 0;
		if (this.getSynopsisID() == 13) {
			for (Object entry : this.getEstimations()) {
				Complex[] coefficients0 = (Complex[]) entry;

				for (Object entry2 : this.getEstimations()) {
					Complex[] coefficients1 = (Complex[]) entry2;

					if (coefficients0 == null || coefficients1 == null) {
						return 0;
					}
					correlation = 1 - distance(coefficients0, coefficients1) / 2;

					if (correlation > Double.parseDouble(this.getParameters()[0]))
						count++;

				}

			}
			return count;
		}
		if (this.getSynopsisID() == 14) {
			for (Object entry : this.getEstimations()) {
				ArrayList<Integer> c0 = (ArrayList<Integer>) entry;

				for (Object entry2 : this.getEstimations()) {
					ArrayList<Integer> c1 = (ArrayList<Integer>) entry2;
					if(c0.size()==c1.size() && c1.size() > 99) {
					double corr = new PearsonsCorrelation().correlation(convertIntegers(c0), convertIntegers(c1));

					if (corr > Double.parseDouble(this.getParameters()[0]))
						System.out.println("Op kati brikame -> " + corr);
					}
				}

			}
			return count;
		}

		return sum;
	}

	private double distance(Complex[] a, Complex[] b) {
		double distance = 0;
		for (int i = 1; i < a.length; i++) { // The first coefficient is always 0
			distance += Math.pow((a[i].getReal() - b[i].getReal()), 2);
			distance += Math.pow((a[i].getImaginary() - b[i].getImaginary()), 2);
		}
		return distance;
	}

	public static double[] convertIntegers(List<Integer> integers) {
		double[] ret = new double[integers.size()];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = integers.get(i).intValue();
		}
		return ret;
	}

}