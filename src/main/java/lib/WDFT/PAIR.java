package lib.WDFT;

import lib.WLSH.SimilarSims;

import java.util.Objects;

public class PAIR implements Comparable<PAIR> {

    private double correlation;
    private String correlatedStream1;
    private String correlatedStream2;
    private COEF coefStream1;
    private COEF coefStream2;

    @Override
    public String toString() {
        return correlatedStream1 + "_" + correlatedStream2 + "_" + correlation +
                "_" + coefStream1.toString() +
                "_" + coefStream2.toString() + "\n";
    }

    public PAIR(double corr, String streamID1, String streamID2, COEF wp1, COEF wp2) {
        correlation = corr;
        correlatedStream1 = streamID1;
        correlatedStream2 = streamID2;
        coefStream1 = wp1;
        coefStream2 = wp2 ;
    }

    public double pearsonCorr() {
        return coefStream1.pearsonCorrelation(coefStream2.getRealValue());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;
        PAIR other = (PAIR)o;
        return this.correlation == other.correlation && this.correlatedStream1.equals(other.correlatedStream1) && this.correlatedStream2.equals(other.correlatedStream2) && this.coefStream1.equals(other.coefStream1) && this.coefStream2.equals(other.coefStream2);
    }

    @Override
    public int hashCode() { return Objects.hash(correlation, correlatedStream1, correlatedStream2, coefStream1, coefStream2); }

    public double getCorrelation() { return correlation; }
    public void setCorrelation(double correlation) { this.correlation = correlation; }

    public String getCorrelatedStream1() { return correlatedStream1; }
    public void setCorrelatedStream1(String correlatedStream) { this.correlatedStream1 = correlatedStream; }

    public String getCorrelatedStream2() { return correlatedStream2; }
    public void setCorrelatedStream2(String correlatedStream) { this.correlatedStream2 = correlatedStream; }

    public COEF getCoefStream1() { return coefStream1; }
    public void setCoefStream1(COEF coefStream1) { this.coefStream1 = coefStream1; }

    public COEF getCoefStream2() { return coefStream2; }
    public void setCoefStream2(COEF coefStream2) { this.coefStream2 = coefStream2; }

    @Override
    public int compareTo(PAIR o) {
        Double s1 = this.getCorrelation();
        Double s2 = o.getCorrelation();
        return s1.compareTo(s2);
    }
}
