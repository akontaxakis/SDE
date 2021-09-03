package infore.SDE.synopses;

import java.util.BitSet;

public class Bitmap {
    private String StreamKey;
    private BitSet bits;
    private double h_weight;

    public Bitmap(String streamKey, BitSet bits, double Hweight) {
        StreamKey = streamKey;
        this.bits = bits;
        h_weight=Hweight;
    }

    public String getStreamKey() {
        return StreamKey;
    }

    public void setStreamKey(String streamKey) {
        StreamKey = streamKey;
    }

    public BitSet getBits() {
        return bits;
    }

    public void setBits(BitSet bits) {
        this.bits = bits;
    }

    public double getHweight() {
        return h_weight;
    }

    public void setHweight(double hweight) {
        this.h_weight = hweight;
    }
}
