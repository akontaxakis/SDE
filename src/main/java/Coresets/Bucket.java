package Coresets;

import java.io.Serializable;

/**
 * This class represents the data structure of the Bucket.
 */

public class Bucket implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// The actual array that contains the points of the coreset
    public Point[] points;

    /*
    A helper array that is used during the reduce procedure when
    the points array is full.
     */
    public Point[] spillover;

    // The number of points that points array currently holds
    public int cursize;

    /*
    Id of the Bucket. The value of this variable follows the properties from
    the original paper. Bucket 0 = input buffer. Bucket with id>0 represents
    (2^(i-1))*m points.
     */
    public int id;

    public Bucket(int id) {
        this.cursize = 0;
        this.id = id;
    }

    public Bucket(int bucketSize, int id) {
        this.points = new Point[bucketSize];
        this.cursize = 0;
        this.id = id;
    }

    /**
     * Returns a string representation of this Bucket with format
     * "points = {[w1 x1 y1 ...],[w2 x2 y2 ...],} sum of weights=..."
     * @return The string representation.
     */
    @Override
    public String toString() {
        int sumOfWeights = 0;
        StringBuilder sb = new StringBuilder();

        sb.append("points = {");
        for (int i=0; i<cursize; i++) {
            sb.append("["+points[i].toString() + "],");
            sumOfWeights += points[i].weight;
        }
        sb.append("} sum of weights="+sumOfWeights);

        return sb.toString();
    }

}