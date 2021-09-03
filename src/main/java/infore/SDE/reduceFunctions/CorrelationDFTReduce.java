package infore.SDE.reduceFunctions;

import infore.SDE.messages.Estimation;
import lib.TimeSeries.COEF;
import org.apache.commons.math3.complex.Complex;

import java.util.ArrayList;

import static java.lang.Math.sqrt;

public class CorrelationDFTReduce extends ReduceFunction{

    ArrayList<COEF> coefs;

    public CorrelationDFTReduce(int nOfP, int count, String[] parameters, int syn, int rq) {
        super(nOfP, count, parameters, syn, rq);
        coefs = new ArrayList<>();
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean add(Estimation e) {
        ArrayList<COEF> k = (ArrayList<COEF>) e.getEstimation();
        count++;
        for(COEF c: k) {
            coefs.add(c);
        }
        if(count==getnOfP()) {
            return true;
        }
        return false;
    }

    @Override
    public Object reduce() {
        int counter=0;
        String str = " ";
        double correlation;

        double th = Double.parseDouble(this.getParameters()[0]);
        int nulls =0;
        int above =0;

        for (COEF coefficients0 : coefs) {
            for (COEF coefficients1 : coefs) {
                if(coefficients0.getStreamKey().compareTo(coefficients1.getStreamKey()) == 1){
                    if (coefficients0.getFourierCoefficients() == null || coefficients1.getFourierCoefficients() == null) {
                        nulls++;
                    } else {
                        if (!(coefficients0.getStreamKey().startsWith((coefficients1.getStreamKey())))) {
                            double dist = distance(coefficients0.getFourierCoefficients(), coefficients1.getFourierCoefficients()) / 2;
                            correlation = 1 - dist;
                            if (correlation > th && correlation < 1) {
                                str = str.concat("[" + coefficients0.getStreamKey() + "_" + coefficients1.getStreamKey() + "_" + correlation + "]");
                                counter++;
                            } else {
                                str = str.concat("[" + coefficients0.getStreamKey() + "_" + coefficients1.getStreamKey() + "_" + correlation + "]");
                                above++;
                            }
                        }
                    }
                }
            }
        }
        if(counter ==0) {
        return str;
        }
        return str;

    }
    private double distance(Complex[] a, Complex[] b) {
            double distance = 0;
            for (int i = 1; i < a.length; i++) { // The first coefficient is always 0
                distance += Math.pow((a[i].getReal() - b[i].getReal()), 2);
                distance += Math.pow((a[i].getImaginary() - b[i].getImaginary()), 2);
            }
        return sqrt(distance);
    }


}
