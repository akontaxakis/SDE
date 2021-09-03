package lib.WLSH;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;

import java.util.List;

public class SimilarSims implements Comparable<SimilarSims> {

    private HashedWindow h1;
    private HashedWindow h2;
    private double HWdiff;
    private double correlation;


    private boolean init;

    public SimilarSims(HashedWindow h1, HashedWindow h2, double hWdiff, double cn) {
        this.h1 = h1;
        this.h2 = h2;
        HWdiff = hWdiff;
        correlation = cn;
        init = false;
    }

    public SimilarSims(HashedWindow window, HashedWindow hashedWindow, double mindiff) {
        this.h1 = window;
        this.h2 = hashedWindow;
        HWdiff = mindiff;
    }


    public boolean isInit() {
        return init;
    }

    public void setInit(boolean init) {
        this.init = init;
    }

    public HashedWindow getSim1() {
        return h1;
    }

    public void setSim1(HashedWindow sim1) {
        this.h1 = sim1;
    }

    public HashedWindow getSim2() {
        return h2;
    }

    public void setSim2(HashedWindow sim2) {
        this.h2 = sim2;
    }

    public double getHWdiff() {
        return HWdiff;
    }

    public void setHWdiff(double HWdiff) {
        this.HWdiff = HWdiff;
    }

    public double getCorrelation() {
        return correlation;
    }

    public void setCorrelation(double correlation) {
        this.correlation = correlation;
    }

    public double computeCorrelation() {
        double w1 = 0.5;
        double w2 = 0.25;
        double w3 = 0.25;
        WLSH lsh1 = h1.getLsh_vector();
        WLSH lsh2 = h2.getLsh_vector();
        double corr1 = returnCorrPerDimension(lsh1, lsh2, 0);
        double corr2 = returnCorrPerDimension(lsh1, lsh2, 1);
        double corr3 = returnCorrPerDimension(lsh1, lsh2, 2);

        correlation = (w1 * corr1) + (w2 * corr2) + (w3 * corr3);
        return correlation;
    }

    private double returnCorrPerDimension(WLSH lsh1, WLSH lsh2, int element) {
        double corr;
        double[] lsh1_array = new double[lsh1.getWindow_data().size()];
        double[] lsh2_array = new double[lsh2.getWindow_data().size()];
        List<int[]> L_k1 = lsh1.getWindow_data();
        List<int[]> L_k2 = lsh2.getWindow_data();
        int i = 0;
        for (int[] el1 : L_k1) {
            lsh1_array[i++] = el1[element];
        }
        int j = 0;
        for (int[] el2 : L_k2) {
            lsh2_array[j++] = el2[element];
        }

        boolean f1 = AreAllSame(lsh1_array);
        if(f1){
            lsh1_array[0] = lsh1_array[0]+1;
        }

        boolean f2 = AreAllSame(lsh2_array);
        if(f2){
            lsh2_array[0] = lsh2_array[0]+1;
        }

        corr = new PearsonsCorrelation().correlation(lsh1_array, lsh2_array);
        //corr = correlationCoefficient(lsh1_array, lsh2_array,n);
        //System.out.println("correlation:"+corr);
        return corr;
    }

    public static boolean AreAllSame(double[] array)
    {
        boolean isFirstElementNull = array[0] == 0;
        for(int i = 1; i < array.length; i++)
        {
            if(isFirstElementNull)
                if(array[i] != 0) return false;
                else
                if(array[0]!=array[i]) return false;
        }

        return true;
    }


    static float correlationCoefficient(double[] X, double[] Y, int n)
    {

        double sum_X = 0, sum_Y = 0, sum_XY = 0;
        double squareSum_X = 0, squareSum_Y = 0;

        for (int i = 0; i < n; i++)
        {
            // sum of elements of array X.
            sum_X = sum_X + X[i];

            // sum of elements of array Y.
            sum_Y = sum_Y + Y[i];

            // sum of X[i] * Y[i].
            sum_XY = sum_XY + X[i] * Y[i];

            // sum of square of array elements.
            squareSum_X = squareSum_X + X[i] * X[i];
            squareSum_Y = squareSum_Y + Y[i] * Y[i];
        }

        // use formula for calculating correlation
        // coefficient.

        return (float)(n * sum_XY - sum_X * sum_Y)/
                (float)(Math.sqrt((n * squareSum_X -
                        sum_X * sum_X) * (n * squareSum_Y -
                        sum_Y * sum_Y)));
    }

    public static boolean areSame(double[] arr) {
        double first = arr[0];
        for (int i = 1; i < arr.length; i++)
            if (arr[i] != first)
                return false;
        return true;
    }


    public String toString() {
        return "{" + h1.getSimulation() + "_" + h1.getStart_time() + " = " + getCorrelation() + "}{" + h2.getSimulation() + "_" + h2.getStart_time() + "}";
    }


    @Override
    public int compareTo(SimilarSims o) {
        Double s1 = this.getCorrelation();
        Double s2 = o.getCorrelation();
        return s1.compareTo(s2);
    }
}
