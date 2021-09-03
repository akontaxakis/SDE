package lib.Coresets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * This class includes all the methods and the data structures that are necessary to create a coreset tree
 * from a given set of points. This class implements the algorithm 4.1 (i.e. TreeCoreset(P,m)) from the
 * original paper.
 */
public final class TreeCoreset implements Serializable{
    
	/**
	 * 
	 */
	
	private static final long serialVersionUID = 1L;

	/**
     * Data structure that represents a single node in the Tree.
     */
    private class TreeNode {

        // Number of points in this node
        int n;

        // Pu (point set)
        Point[] points;

        // qu representative point of the tree node (centre)
        Point centre;

        // Weighted coordinates of the centre
        double[] centreCoordinatesW;

        // Left child node
        TreeNode lc;

        // Right child node
        TreeNode rc;

        // Parent node
        TreeNode parent;

        // Cost of the tree node
        double cost;

        /*
        Possible values = [root, l for left, r for right].
        The left child represents the old centre and the right
        represents the new centre.
         */
        String type;

        // Number of dimensions
        int d;

        /**
         * Constructor that is used to create inner or leaf nodes.
         * @param points Actual data points.
         * @param centre Centre point.
         * @param parent Parent node.
         * @param type Type of the node (l or r).
         * @param d Number of dimensions.
         */
        private TreeNode(Point[] points, Point centre, TreeNode parent, String type, int d) {
            n = points.length;
            this.points = points;
            this.centre = centre;
            lc = null;
            rc = null;
            this.parent = parent;
            this.type = type;
            this.d = d;

            // Calculate the weighted coordinates of the centre
            if(type.equals("r")) {
                // Right child, so new centre
                centreCoordinatesW = new double[d];
                for (int l = 0; l < d; l++) {
                    centreCoordinatesW[l] = centre.coordinates[l] / centre.weight;
                }
            }
            else{
                // Left child, so old centre
                centreCoordinatesW = parent.centreCoordinatesW;
            }

            // Calculate node cost
            cost = treeNodeCost();
        }

        /**
         * Constructor that is used to create the root of the tree.
         * @param set_1 First set of points.
         * @param set_2 Second set of points.
         * @param centre The first representative point.
         * @param centreIndex Index of the centre.
         * @param d Number of dimensions.
         */
        private TreeNode(Point[] set_1, Point[] set_2, Point centre, int centreIndex, int d){
            n = set_1.length + set_2.length;
            this.centre = centre;
            lc = null;
            rc = null;
            parent = null;
            type = "root";
            this.d = d;

            // Set center index and concatenate the two point sets
            points = new Point[n];
            int n_1 = set_1.length;
            for(int i=0; i<n_1; i++){
                set_1[i].centreIndex = centreIndex;
                points[i] = set_1[i];
            }
            for(int i=n_1; i<n; i++){
                set_2[i-n_1].centreIndex = centreIndex;
                points[i] = set_2[i-n_1];
            }

            // Calculate the weighted coordinates of the centre
            centreCoordinatesW = new double[d];
            for(int l=0; l<d; l++) {
                centreCoordinatesW[l] = centre.coordinates[l] / centre.weight;
            }

            // Calculate node cost
            cost = treeNodeCost();
        }

        /**
         * Calculates the cost of the node (sum of squared distances
         * between the centre and the points of the node).
         * @return The cost.
         */
        private double treeNodeCost(){
            double sum = 0.0;
            for(int i=0; i<n; i++){
                sum += treeNodeCostOfPoint(points[i], centreCoordinatesW , d);
            }
            return sum;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append("t="+type);
            sb.append(", n="+n);
            sb.append(", c="+cost);

            if(this.rc == null && this.lc == null){
                sb.append(", leaf id="+centre.id+", centre w="+centre.weight);
                for(int i=0; i<n; i++){
                    if(points[i].id == centre.id){
                        // True if centre is within the points array (this is correct)
                        sb.append(", true");
                    }
                }
            }else{
                sb.append(", NO leaf");
            }

            return sb.toString();
        }
    }

    /**
     * Calculates the squared distance between a given point and a centre point.
     * @param p The Point.
     * @param centreCoordinatesW Vector that contains the weighted coordinates of the centre.
     * @param d Number of dimensions.
     * @return Returns the squared distance.
     */
    private static double treeNodeCostOfPoint(Point p, double[] centreCoordinatesW, int d){
        double distance = 0.0;

        for(int l=0; l<d; l++){
            double centroidCoordinatePoint = p.coordinates[l] / p.weight;
            distance += Math.pow(centroidCoordinatePoint-centreCoordinatesW[l],2);
        }
        return distance * p.weight;
    }

    /**
     * Determines which of the two centres is nearest to the given point.
     * @param p The point we want to examine.
     * @param centreACoordinatesW Vector with the weighted coordinates of the first centre (old centre).
     * @param centreB Second centre (new centre).
     * @param d Number of dimensions.
     * @return Returns True if centreB is nearest to the given point.
     */
    private static boolean determineNearestCentre(Point p, double[] centreACoordinatesW, Point centreB, int d){

        double distanceA = 0.0;
        double distanceB = 0.0;

        for(int l=0; l<d; l++){
            double centroidCoordinatePoint = p.coordinates[l] / p.weight;
            distanceA += Math.pow(centroidCoordinatePoint-centreACoordinatesW[l],2);
            double centroidCoordinateCentreB = centreB.coordinates[l] / centreB.weight;
            distanceB += Math.pow(centroidCoordinatePoint-centroidCoordinateCentreB,2);
        }

        // Return the nearest centre
        if(distanceA < distanceB){
            return false; //centreA (old centre)
        } else {
            return true; //centreB (new centre)
        }

    }

    /**
     * Calculates the total sum of the squared distances between the points of the node
     * and the nearest center (i.e. the old and the new center).
     * @param node The node that is going to be splitted.
     * @param centreACoordinatesW Vector with the weighted coordinates of the old center.
     * @param centreB New center.
     * @param d Number of dimensions.
     * @return The total sum.
     */
    private static double treeNodeSplitCost(TreeNode node, double[] centreACoordinatesW, Point centreB, int d){
        double sum = 0.0;

        for(int i=0; i<node.n; i++){

            double distanceA = 0.0;
            double distanceB = 0.0;

            for(int l=0; l<d; l++){
                double centroidCoordinatePoint = node.points[i].coordinates[l] / node.points[i].weight;
                distanceA += Math.pow(centroidCoordinatePoint-centreACoordinatesW[l],2);
                double centroidCoordinateCentreB = centreB.coordinates[l] / centreB.weight;
                distanceB += Math.pow(centroidCoordinatePoint-centroidCoordinateCentreB,2);
            }

            if(distanceA < distanceB){
                sum += distanceA*node.points[i].weight;
            } else {
                sum += distanceB*node.points[i].weight;
            }
        }

        return sum;
    }

    /**
     * Checks whether a node is a leaf node.
     * @param node Node to be checked.
     * @return Returns True for the leaf nodes.
     */
    private static boolean isLeaf(TreeNode node){
        if(node.lc == null && node.rc == null){
            return true;
        } else {
            return false;
        }

    }

    /**
     * This function starts from the root of the tree and selects iteratively one of the
     * two child nodes at random according to their weights until a leaf is chosen.
     * @param root Root node of the tree.
     * @param random Random generator with predefined seed.
     * @return Returns the selected leaf node.
     */
    private static TreeNode selectNode(TreeNode root, Random random){
        TreeNode node = root;
        double randomDouble = random.nextDouble();

        while(!isLeaf(node)){
            if(node.lc.cost == 0 && node.rc.cost == 0){
                /*
                Cases where the cost is zero. This happens when the node has
                only one point (which is the centre it self) or when it has multiple
                points with exactly the same coordinates.
                 */
                if(node.lc.n == 1 && node.rc.n == 1){
                    node = root;
                    randomDouble = random.nextDouble();
                }else if(node.lc.n == 1){
                    node = node.rc;
                } else if(node.rc.n == 1){
                    node = node.lc;
                }else if(randomDouble < 0.5){
                    node = node.lc;
                    randomDouble = random.nextDouble();
                } else {
                    node = node.rc;
                    randomDouble = random.nextDouble();
                }
            } else {
                // Standard cases
                if (randomDouble < node.lc.cost / node.cost) {
                    node = node.lc;
                } else {
                    node = node.rc;
                }
            }
        }

        return node;
    }

    /**
     * Chooses the next representative point from the given leaf node at random according
     * to its squared distance to the representative point of the leaf node.
     * @param node The leaf node of the tree.
     * @param d Number of dimensions.
     * @param random Random generator with predefined seed.
     * @return Returns the next representative point.
     */
    private static Point chooseCentre(TreeNode node, int d, Random random){

        /*
        The number of trials that we want to apply the following procedure in order to choose a better
        representative point. This is a simple heuristic that helps us to choose a representative point
        that has lower cost.
         */
        int times = 3;

        // Stores the best center that has been chosen so far
        Point bestCentre = null;

        // Stores the cost of the node that has been splitted with the best centre
        double minCost = node.cost;

        if(node.cost == 0){
            // This case exists when all points in a node are exactly the same
            for (int i = 0; i < node.n; i++) {
                if (node.points[i].id != node.centre.id) {
                    return node.points[i];
                }
            }
        }else{
            for(int j=0; j<times; j++){

                double sum = 0.0;
                double randomDouble = random.nextDouble();

                for(int i=0; i<node.n; i++){
                    // Choose the next representative point according to the probability
                    sum += treeNodeCostOfPoint(node.points[i], node.centreCoordinatesW , d) / node.cost;
                    if(sum >= randomDouble){
                        // Calculate the cost of the splitting procedure to see if there is a better representative point
                        double curCost = treeNodeSplitCost(node, node.centreCoordinatesW, node.points[i], d);
                        if(curCost < minCost){
                            bestCentre = node.points[i];
                            minCost = curCost;
                        }
                        break;
                    }
                }
            }
        }

        // This case exists when curCost > minCost
        if(bestCentre == null){
            while(bestCentre == null) {
                double randomDouble = random.nextDouble();
                double sum = 0.0;
                for (int i = 0; i < node.n; i++) {
                    sum += (i + 1) / node.n;
                    if (sum >= randomDouble && node.points[i].id != node.centre.id) {
                        bestCentre = node.points[i];
                    }
                }
            }

        }

        return bestCentre;
    }

    /**
     * Splits the parent node based on the old and new representative points and then creates two child nodes.
     * Then it assigns to each child node the points that are nearest to each representative point.
     * @param parent The parent node that is going to be splitted.
     * @param newCentre The new representative point of the parent node.
     * @param newCentreIndex The index of the new representative point.
     * @param d The number of dimensions.
     */
    private static void split(TreeNode parent, Point newCentre, int newCentreIndex, int d){

        // Points nearest to the old centre
        ArrayList<Point> oldPointsList = new ArrayList<>();
        // Points nearest to the new centre
        ArrayList<Point> newPointsList = new ArrayList<>();

        // Assign every point of the node to its nearest centre
        int oldCentreIndex = parent.points[0].centreIndex;
        for(int i=0; i<parent.n; i++){
            boolean centreBool = determineNearestCentre(parent.points[i], parent.centreCoordinatesW, newCentre, d);
            if(centreBool == true){
                parent.points[i].centreIndex = newCentreIndex;
                newPointsList.add(parent.points[i]);
            } else{
                oldPointsList.add(parent.points[i]);
            }
        }

        Point[] oldPoints;
        Point[] newPoints;
        // Spacial case, this case exists when all points in a node are exactly the same
        if(oldPointsList.size() == 0){

            // Create two equal size arrays
            oldPoints = new Point[newPointsList.size()/2];
            if ((newPointsList.size() % 2) == 1){
                newPoints = new Point[newPointsList.size()/2 +1];
            }else{
                newPoints = new Point[newPointsList.size()/2];
            }

            // Store old centre to old array and new centre to new array
            for(Point p : newPointsList){
                if(p.id == parent.centre.id){
                    p.centreIndex = oldCentreIndex;
                    oldPoints[0] = p;
                }else if(p.id == newCentre.id){
                    p.centreIndex = newCentreIndex;
                    newPoints[0] = p;
                }
            }

            // Split in half the rest of the points
            int count = 1;
            for(Point p : newPointsList){
                if(p.id != parent.centre.id && p.id != newCentre.id && count < newPointsList.size()/2){
                    p.centreIndex = oldCentreIndex;
                    oldPoints[count] = p;
                    count++;
                }
                else if(p.id !=parent.centre.id && p.id != newCentre.id){
                    p.centreIndex = newCentreIndex;

                    int index = count+1-(newPointsList.size()/2);

                    newPoints[index] = p;
                    count++;
                }
            }
        }else{
            // Convert lists to arrays of fixed size
            oldPoints = oldPointsList.toArray(new Point[oldPointsList.size()]);
            newPoints = newPointsList.toArray(new Point[newPointsList.size()]);
        }

        // Create the left child = old centre
        TreeNode lc = new TreeCoreset().new TreeNode(oldPoints, parent.centre, parent,"l", d);

        // Create the right child = new centre
        TreeNode rc = new TreeCoreset().new TreeNode(newPoints, newCentre, parent, "r", d);

        // Set the childrens of the parent node
        parent.lc = lc;
        parent.rc = rc;
        parent.points = null;
        parent.centre = null;
        parent.centreCoordinatesW = null;

        // Propagate the cost changes to the parent nodes
        while(parent != null){
            parent.cost = parent.lc.cost + parent.rc.cost;
            parent = parent.parent;
        }

    }

    /**
     * Creates a coreset of size m from the union of two sets of points.
     * Implements the algorithm 4.1 (i.e. TreeCoreset(P,m)) from the original paper.
     * @param set_1 First set of points.
     * @param set_2 Second set of points.
     * @param m Size of the produced coreset.
     * @param d The number of dimensions.
     * @param random Random generator with predefined seed.
     * @return The produced coreset.
     */
    public static Point[] unionTreeCoreset(Point[] set_1, Point[] set_2, int m, int d, Random random){
        int n_1 = set_1.length;
        int n_2 = set_2.length;

        // Initializing array for the result
        Point[] coreset = new Point[m];

        // Total number of points
        int n = n_1 + n_2;
        // Set point id to distinguish the points during the selection procedure
        for(int i=0; i<n; i++){
            if (i < n_1) {
                set_1[i].id = i;
            }else{
                set_2[i-n_1].id = i;
            }
        }

        // Number of chosen centers
        int chosenPoints = 0;

        // Choose the first centre q1 uniformly at random from P = set_1 UNION set_2
        int j = random.nextInt(n);
        if(j < n_1){
            coreset[chosenPoints] = new Point(set_1[j].coordinates.clone(), set_1[j].weight, set_1[j].id);
        }
        else{
            coreset[chosenPoints] = new Point(set_2[j - n_1].coordinates.clone(), set_2[j - n_1].weight, set_2[j - n_1].id);
        }

        // Construct the root node with qroot = q1 and weight(root) = cost(P,q1)
        TreeNode root = new TreeCoreset().new TreeNode(set_1, set_2, coreset[chosenPoints], chosenPoints, d);
        chosenPoints++;

        // Choose the remaining points q2...qk
        while(chosenPoints < m){
            // Select the node to split
            TreeNode leaf = selectNode(root, random);
            // Choose the next representative point
            Point centre = chooseCentre(leaf, d, random);
            Point newCentre = new Point(centre.coordinates.clone(), centre.weight, centre.id);
/*
            for(int i=0;i<chosenPoints;i++){
                if(coreset[i].id==newCentre.id){
                    System.out.println("@@@@@@ same id@@@@@@="+newCentre.id );
                }
            }
*/
            // Split the selected node to two child nodes
            split(leaf, newCentre, chosenPoints, d);
            coreset[chosenPoints] = newCentre;
            chosenPoints++;
        }

        root=null;

        /*
        We recalculate the coordinates of the chosen representative points as the mean
        value of the points that are nearest to each representative point.
         */
        for(int i=0; i<n; i++) {
            if (i < n_1) {
                int index = set_1[i].centreIndex;
                if (coreset[index].id != set_1[i].id) {
                    coreset[index].weight += set_1[i].weight;
                    for (int l = 0; l<d; l++) {
                        coreset[index].coordinates[l] += set_1[i].coordinates[l];
                    }
                }
            } else {
                int index = set_2[i - n_1].centreIndex;
                if (coreset[index].id != set_2[i - n_1].id) {
                    coreset[index].weight += set_2[i - n_1].weight;
                    for (int l = 0; l<d; l++) {
                        coreset[index].coordinates[l] += set_2[i - n_1].coordinates[l];
                    }
                }
            }
        }

        return coreset;
    }

}