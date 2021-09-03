package lib.Coresets;

import java.io.Serializable;
import java.util.Random;

/**
 * This class includes all the methods for applying the K-Means++ algorithm
 * on the final coreset to find the set of cluster centres.
 */
public final class KmeansPP implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
     * Calculates the squared distance between a point and a centroid.
     * @param point The point.
     * @param centroid The centroid.
     * @param d Number of dimensions.
     * @return Returns the squared distance.
     */
    public static double costOfPointToCentroid(Point point, Point centroid, int d){
        double distance = 0.0;

        for(int l=0; l<d; l++){
            double centroidCoordinateOfPoint = point.coordinates[l] / point.weight;
            double centroidCoordinate = centroid.coordinates[l] / centroid.weight;

            distance += Math.pow(centroidCoordinateOfPoint-centroidCoordinate, 2);
        }
        return distance;
    }

    /**
     * Determines the nearest centroid between a point and a vector of centroids.
     * @param point The point.
     * @param centroids Array with centroids.
     * @param d Number of dimensions.
     * @return The index of the nearest centroid.
     */
    public static int determineNearestCentroid(Point point, Point[] centroids, int d){

        int centroidIndex = 0;
        double nearestCost = -1.0;

        for(int i=0; i<centroids.length; i++){
            double distance = costOfPointToCentroid(point, centroids[i], d);
            if(nearestCost <0 || distance < nearestCost) {
                nearestCost = distance;
                centroidIndex = i;
            }
        }

        return centroidIndex;
    }

    /**
     * Calculates the cost of the clustering, that is the minimum sum
     * of squared distances between a set of points and a set of centroids.
     * @param centroids Array with centroids.
     * @param points Array with points.
     * @param d Number of dimensions.
     * @return Returns the sum of squared distances.
     */
    private static double costOfKmeansPP(Point[] centroids, Point[] points, int d){

        double sum = 0.0;

        for(int i=0; i<points.length; i++){
            double nearestCost = -1.0;
            for(int j=0; j<centroids.length; j++){
                double distance = costOfPointToCentroid(points[i], centroids[j], d);
                if(nearestCost <0 || distance < nearestCost) {
                    nearestCost = distance;
                }
            }
            sum += nearestCost*points[i].weight;
        }

        return sum;
    }

    /**
     * This method applies the K-Means++ initialization procedure to find the
     * initial values for the seeds (centroids) from a set of points.
     * @param points Array with points.
     * @param k Number of centroids.
     * @param d Number of dimensions.
     * @param random Random generator with predefined seed.
     * @return Returns the array with the initial seeds (centroids).
     */
    private static Point[] centroidsInitialization(Point[] points, int k, int d, Random random){

        // Initialize k centroids
        Point[] centroids = new Point[k];

        // Array that stores the cost of each point to its nearest centroid
        double [] costOfPoints = new double[points.length];

        // Choose the first centroid uniformly at random from the set of points
        int randomInt = random.nextInt(points.length);
        centroids[0] = new Point(points[randomInt].coordinates.clone(), points[randomInt].weight);

        // Assign each point to the first centroid and calculate the cost to the centroid
        for(int i=0; i<points.length; i++){
            //points[i].centreIndex = 0;
            costOfPoints[i] = costOfPointToCentroid(points[i], centroids[0], d)*points[i].weight;
        }

        // Choose the remaining to k-1 centroids with K-Means++ seeding distribution
        for(int i=1; i<k; i++){

            // Calculate the total cost
            double totalCost = 0.0;
            for(double cost: costOfPoints){
                totalCost += cost;
            }

            /*
            Choose the next centroid at random from the set of points with
            probability proportional to the squared distance of each point
            to the nearest centroid.
             */
            double randomDouble = random.nextDouble();
            double sum = 0.0;
            int index = -1;
            for(int j=0; j<points.length; j++){
                sum += costOfPoints[j];
                if(randomDouble <= sum/totalCost){
                    index = j;
                    break;
                }
            }
            centroids[i] = new Point(points[index].coordinates.clone(), points[index].weight);

            // Check which points are nearest to the new centre
            for(int j=0; j<points.length; j++){
                double newCost = costOfPointToCentroid(points[j], centroids[i], d)*points[j].weight;
                if(costOfPoints[j] > newCost){
                    costOfPoints[j] = newCost;
                    //points[j].centreIndex = i;
                }
            }

        }

        return centroids;
    }

    /**
     * Applies the K-Means++ algorithm to find the cluster centres from a set
     * of points. This method uses at first the initialization procedure
     * of K-Means++ and then applies a number of fixed iterations the K-Means
     * algorithm to find the final values of the cluster centers.
     * @param points Array with points.
     * @param km_iterations Number of iterations.
     * @param k Number of centroids.
     * @param d Number of dimensions.
     * @param random Random generator with predefined seed.
     * @return Returns the array with the cluster centres.
     */
    private static Point[] findClusterCentroidsIterations(Point[] points, int km_iterations, int k, int d, Random random){
        // Initialization procedure of K-Means++
        Point[] centroids = centroidsInitialization(points, k, d ,random);

        // Applies #iterations the K-Means algorithm
        for(int loop=0; loop<km_iterations; loop++){
            // Find the nearest centroids
            for(int i=0;i<points.length;i++){
                points[i].centreIndex = determineNearestCentroid(points[i], centroids, d);
            }

            for(int i=0;i<k;i++){
                // Clear centroid data
                centroids[i].weight = 0;
                for(int l=0; l<d; l++){
                    centroids[i].coordinates[l] = 0;
                }

                // Recalculate the mean values of the centroids.
                for (int j=0; j<points.length; j++){
                    if(points[j].centreIndex == i){
                        centroids[i].weight += points[j].weight;

                        for(int l=0; l<d; l++){
                            centroids[i].coordinates[l] += points[j].coordinates[l];
                        }
                    }
                }
            }

        }

        return centroids;
    }

    /**
     * Applies the K-Means++ algorithm to find the cluster centres from a set
     * of points. This method uses at first the initialization procedure
     * of K-Means++ and then applies multiple times the K-Means algorithm until
     * the mean values of the cluster centres do not change.
     * @param points Array with points.
     * @param k Number of centroids.
     * @param d Number of dimensions.
     * @param random Random generator with predefined seed.
     * @return Returns the array with the cluster centres.
     */
    private static Point[] findClusterCentroidsConvergence(Point[] points, int k, int d, Random random){
        // Initialization procedure of K-Means++
        Point[] centroids = centroidsInitialization(points, k, d, random);
        // Stores the overall sum of squared distances for a given set of centroids
        double curCost = costOfKmeansPP(centroids, points, d);
        double newCost = curCost;

        // Applies the K-Means algorithm until the mean values of the cluster centres do not change
        do {
            curCost = newCost;

            // Find the nearest centroids
            for(int i=0; i<points.length; i++){
                points[i].centreIndex = determineNearestCentroid(points[i], centroids, d);
            }

            for(int i=0; i<k; i++){
                // Clear centroid data
                centroids[i].weight=0;
                for(int l=0; l<d; l++){
                    centroids[i].coordinates[l] = 0;
                }

                // Recalculate the mean values of the centroids.
                for (int j=0; j<points.length; j++){
                    if(points[j].centreIndex == i){
                        centroids[i].weight += points[j].weight;

                        for(int l=0; l<d; l++){
                            centroids[i].coordinates[l] += points[j].coordinates[l];
                        }
                    }
                }
            }

            newCost = costOfKmeansPP(centroids, points, d);
        }while(newCost < curCost);

        return centroids;
    }

    /**
     * This is the general method which applies the K-Means++ algorithm to find
     * the set of cluster centres from a set of points. We give the option to
     * apply multiple times the K-Means++ on the set of points in order to extract
     * a better clustering with lower cost (as the cost depends on the initial
     * values of the centres). Also we give the option to specify at each individual
     * application of K-Means whether the algorithm will run for a fixed number of
     * iterations or until the values of the cluster centres do not change.
     * @param points Array with points.
     * @param km_applications Number of applications of K-Means++.
     * @param km_iterations Number of iterations of each individual application of K-Means++
     *                   (if iterations<=0 then K-Means runs until cluster centres do not change).
     * @param k Number of centroids.
     * @param d Number of dimensions.
     * @param random Random generator with predefined seed.
     * @return Returns the array with the cluster centres.
     */
    public static Point[] applyKmeansPP(Point[] points, int km_applications, int km_iterations, int k, int d, Random random){
        // Stores the final centroids
        Point[] centroids;

        // Apply multiple times the K-Means++
        if(km_iterations <= 0){
            centroids = findClusterCentroidsConvergence(points, k, d, random);
        }else{
            centroids = findClusterCentroidsIterations(points, km_iterations, k, d, random);
        }
        double minCost = costOfKmeansPP(centroids, points, d);


        for(int i=1; i<km_applications; i++){
            Point[] tempCentroids;
            if(km_iterations <= 0){
                tempCentroids = findClusterCentroidsConvergence(points, k, d, random);
            }else{
                tempCentroids = findClusterCentroidsIterations(points, km_iterations, k, d, random);
            }
            double curCost = costOfKmeansPP(tempCentroids, points, d);

            if(curCost < minCost) {
                minCost = curCost;
                centroids = tempCentroids;
            }
        }

        return centroids;
    }

}