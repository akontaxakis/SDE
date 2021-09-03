package lib.WDFT;

import org.apache.commons.math3.complex.Complex;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;

import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

public class COEF {

    private String bucketID;
    private String neighbors;
    private String streamID;

    @Override
    public String toString() {
        return  Arrays.toString(realValue)
//                " stream id=" + streamID +
//                ", windowTime=" + windowTime +
//                ", bucket id=" + bucketID +
                ;
    }

    private Date windowTime;
    private Complex[] dft;
    private String[] realValue;

    public COEF(Date currTime, String sID, Complex[] normalizedFourierCoefficients, String[] rValue) {
        streamID = sID;
        windowTime = currTime;
        dft = normalizedFourierCoefficients;
        realValue = rValue;
    }
    //Calculation of BucketID
    public String keyHash(double threshold) {
        double epsilon = Math.sqrt(1 - threshold);
        int hashOffset = (int) Math.ceil(Math.sqrt(2) / 2);
        String stringKey = "";
        int tmpIndex;
        for (int i = 1; i < 2; i++) {
            tmpIndex = (int) Math.ceil((dft[i].getReal() + hashOffset) / epsilon);
            stringKey += tmpIndex;
            stringKey += ",";

            tmpIndex = (int) Math.ceil(dft[i].getImaginary() + hashOffset / epsilon);
            stringKey += tmpIndex;
            if (i < 2 - 1) {
                stringKey += ",";
            }
        }
        // 1 - maxBucketID
        return stringKey;
    }

    public double pearsonCorrelation(String[] otherValue) {
        PearsonsCorrelation pr = new PearsonsCorrelation();
        double[] tmp1 = Arrays.stream(realValue)
                .mapToDouble(Double::parseDouble)
                .toArray();
        double[] tmp2 = Arrays.stream(otherValue)
                .mapToDouble(Double::parseDouble)
                .toArray();
        if (checkIfSame(tmp1) || checkIfSame(tmp2)) {
            if (Arrays.equals(otherValue, realValue))
                return 1;
            else
                return 0;
        }
        //System.out.println("Pearson: " + pr.correlation(tmp1, tmp2) + Arrays.toString(tmp1) + "\n" + Arrays.toString(tmp2));
        return pr.correlation(tmp1, tmp2);
    }

    private boolean checkIfSame(double[] array) {
        if(array.length == 0) {
            throw new IllegalArgumentException("Array is empty");
        }
        double first = array[0];
        for (double v : array) {
            if (v != first) {
                return false;
            }
        }
        return true;
    }

    public void spearmanCorrelation(String[] otherValue) {
        SpearmansCorrelation pr = new SpearmansCorrelation();
        double[] tmp1 = {Double.parseDouble(Arrays.toString(realValue))};
        double[] tmp2 = {Double.parseDouble(Arrays.toString(otherValue))};
        double correlation = pr.correlation(tmp1, tmp2);
        System.out.println("Spearman correlation: " + correlation);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || this.getClass() != o.getClass())
            return false;
        COEF other = (COEF)o;
        return this.bucketID.equals(other.bucketID) && this.neighbors.equals(other.neighbors) && this.streamID.equals(other.streamID) && this.windowTime.equals(other.windowTime) && Arrays.equals(this.dft, other.dft);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketID, neighbors, streamID, windowTime, Arrays.hashCode(dft));
    }

    public String getBucketID() {
        return bucketID;
    }
    public void setBucketID(String streamID) { this.bucketID = streamID; }

    public String getNeighbors() { return neighbors; }
    public void setNeighbors(String neighbors) { this.neighbors = neighbors; }

    public String getStreamID() { return streamID; }
    public void setStreamID(String streamID) { this.streamID = streamID; }

    public Complex[] getDft() { return dft; }
    public void setDft(Complex[] dft) { this.dft = dft; }

    public Date getWindowTime() { return windowTime; }
    public void setWindowTime(Date windowTime) { this.windowTime = windowTime; }
    public String[] getRealValue() { return realValue; }
}
