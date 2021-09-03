package lib.Coresets;

import java.io.Serializable;

/**
 * This class represents a single point in the Euclidean Space.
 */
public class Point implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	// Coordinates of the point
    public double [] coordinates;

    // Weight attribute of the point
    public int weight;

    // The index of the centre to which the point belongs
    public int centreIndex;

    // The id of the point (the value is not unique, helper variable)
    public int id;

    // If True then no more data follows from the stream after this point
    public boolean eofPoint;

    public Point() {}

    public Point(double [] coordinates) {
        this.coordinates = coordinates;
        this.weight = 1;
        this.centreIndex = -1;
        this.id = -1;
        this.eofPoint = false;
    }

    public Point(boolean eofPoint) {
        this.coordinates = null;
        this.weight = -1;
        this.centreIndex = -1;
        this.id = -1;
        this.eofPoint = eofPoint;
    }

    public Point(double [] pointData, int weight) {
        this.coordinates = pointData;
        this.weight = weight;
        this.centreIndex = -1;
        this.id = -1;
        this.eofPoint = false;
    }

    public Point(double [] pointData, int weight, int id) {
        this.coordinates = pointData;
        this.weight = weight;
        this.centreIndex = -1;
        this.id = id;
        this.eofPoint = false;
    }

    /**
     * Returns a string representation of this point with format "weight x y z ..."
     * @return The string representation.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if(this.eofPoint == false){
            sb.append(weight);
            for(int i=0; i<coordinates.length; i++){
                sb.append(" "+coordinates[i]);
            }
        }else{
            sb.append("EOF Point");
        }

        return sb.toString();
    }
}