package infore.SDE.synopses;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.math3.complex.Complex;
import org.apache.flink.api.java.tuple.Tuple2;

import infore.SDE.messages.Estimation;
import infore.SDE.messages.Request;
import infore.SDE.sketches.TimeSeries.windowDFT;

public class DFT extends Synopsis {
	windowDFT ts;
	int index = 1;

	public DFT(int uid, String[] parameters) {
		super(uid, parameters[0], parameters[1]);
		ts = new windowDFT(Integer.parseInt(parameters[2]), Integer.parseInt(parameters[3]),
				Integer.parseInt(parameters[4]), 1);
	}

	@Override
	public void add(Object k) {
		String j = (String) k;
		String[] tokens = j.split(",");
		ts.pushToValues(Double.parseDouble(tokens[this.valueIndex]));
	}

	@Override
	public Object estimate(Object k) {
		return ts.getNormalizedFourierCoefficients();
	}

	@Override
	public Synopsis merge(Synopsis sk) {
		return null;
	}

	public Tuple2<Integer, Complex[]> indexedEstimation(Object k) {
		return new Tuple2<>(ts.keyHash((double) k), ts.getNormalizedFourierCoefficients());
	}

	public Complex[] getCOEF() {
		return ts.getNormalizedFourierCoefficients();
	}

	public String COEFtoString() {
		int COEFFICIENTS_TO_USE = 2;
		Complex[] fourierCoefficients = ts.getNormalizedFourierCoefficients();
		String answer = " ";
		for (int m = 1; m < COEFFICIENTS_TO_USE; m++) {
			answer = answer + fourierCoefficients[m].getReal() + "  ";

			answer = answer + fourierCoefficients[m].getImaginary() + "  ";
			if (ts.getM() > 1.4)
				answer = answer + " ERROR// " + ts.getM();
			else
				answer = answer + " // " + ts.getM();
		}

		return answer;
	}

	@Override
	public Estimation estimate(Request rq) {
		// TODO Auto-generated method stub
		return new Estimation(rq, getCOEF(), Integer.toString(rq.getUID()));
	}

	public HashSet<String> getKeys(double threshold) {

		double epsilon = Math.sqrt(1 - threshold);
		int hashOffset = (int) Math.ceil(Math.sqrt(2) / (epsilon * 2));

		String key = ts.keyStringHash(threshold);
		String[] keys = key.split(",");

		int[] array = Arrays.asList(keys).stream().mapToInt(Integer::parseInt).toArray();

		HashSet<String> keyList = new HashSet<String>();

		return findAllKeys(0, array, keyList, hashOffset);

	}

	public ArrayList<String> getKeys2(double threshold) {

		double epsilon = Math.sqrt(1 - threshold);
		int hashOffset = (int) Math.floor(Math.sqrt(2) / (epsilon));

		String key = ts.keyStringHash(threshold);
		String[] keys = key.split(",");

		int[] array = Arrays.asList(keys).stream().mapToInt(Integer::parseInt).toArray();
		ArrayList<String> keyList = new ArrayList<String>();

		int tempx, tempy;

		for (int i = -1; i < 2; i++) {
			for (int j = -1; j < 2; j++) {
				tempx = array[0] + i;
				tempy = array[1] + j;

			if (tempx < hashOffset*2 & tempx > 0 & tempy < hashOffset*2 & tempy > 0)
				keyList.add(tempx + "," + tempy);

		    }
		}
		
		return keyList;

	}

	public HashSet<String> findAllKeys(int idx, int[] array, HashSet<String> keyList, int hashOffset) {
		String key;
		array[idx] -= 1;
		if (array[idx] >= 0) {
			if (idx == array.length - 1) {
				key = "";
				for (int j = 0; j < array.length; j++) {
					key += array[j];
					if (j < array.length - 1) {
						key += ",";
					}
				}
				keyList.add(key);
			} else {
				findAllKeys(idx + 1, array, keyList, hashOffset);
			}
		}

		array[idx] += 1;
		if (array[idx] < 2 * hashOffset) {
			if (idx == array.length - 1) {
				key = "";
				for (int j = 0; j < array.length; j++) {
					key += array[j];
					if (j < array.length - 1) {
						key += ",";
					}
				}
				keyList.add(key);
			} else {
				findAllKeys(idx + 1, array, keyList, hashOffset);
			}
		}
		array[idx] += 1;
		if (array[idx] < 2 * hashOffset) {
			if (idx == array.length - 1) {
				key = "";
				for (int j = 0; j < array.length; j++) {
					key += array[j];
					if (j < array.length - 1) {
						key += ",";
					}
				}
				keyList.add(key);
			} else {
				findAllKeys(idx + 1, array, keyList, hashOffset);
			}
		}
		array[idx] -= 1;
		return keyList;
	}

}
