package lib.Coresets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Random;

/**
 * The BucketManager implements the merge-and-reduce technique and holds at any point in time
 * at most O(log2(n/m)) buckets, where n is the number of consumed points from the input stream
 * and m is the maximum number of points that a specific bucket can hold.
 */
public class BucketManager implements Serializable {

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final int INDEX_0 = 0;
    private static final int INDEX_1 = 1;

    /*
    Stores the list of buckets. At any point in time
    this list holds the Bucket 0 and any other FULL Bucket.
     */
    private ArrayList<Bucket> bucketsList;
    private final int MAX_BUCKET_SIZE;
    private final int d;
    private Random random;

    /**
     * Constructs a new BucketManager and initializes a new list of buckets with an empty bucket 0.
     * @param maxBucketSize The maximum number of points that each individual bucket can hold.
     * @param d The number of dimensions of the data points.
     * @param random Random generator with predefined seed.
     */
    public BucketManager(int maxBucketSize, int d, Random random) {
        this.MAX_BUCKET_SIZE = maxBucketSize;
        this.d = d;
        this.random = random;

        bucketsList = new ArrayList<Bucket>(){
            /*
            This reassures that the index of the get method is the same as the
            id of the Bucket (i.e. there is no way that bucketsList.get(2) gives
            a bucket with id!=2.
             */
            @Override public Bucket get(int index)
            {
                if(super.get(index).id == index) {
                    return super.get(index);
                }
                else {
                    throw new IllegalArgumentException("BucketManager: ArrayList get(index).id != index");
                }
            }
        };

        bucketsList.add(new Bucket(maxBucketSize,0));
    }

    /**
     * Implements the algorithm 4.2 InsertPoint(p) from the original paper. Inserts a single
     * point to the first bucket (i.e. Bucket 0) according to merge-and-reduce technique.
     * @param point The input point from the stream of points.
     */
    public void insertPoint(Point point){

        // If Bucket 0 is full check the next bucket (i.e. Bucket 1)
        if (bucketsList.get(INDEX_0).cursize == MAX_BUCKET_SIZE){

            int curBucket  = 0;
            int nextBucket = 1;

            // If there is no Bucket 1, make a new one
            if (bucketsList.size() == 1){
                bucketsList.add(new Bucket(INDEX_1));
            }

            // If Bucket 1 is empty, move the points from Bucket 0 to Bucket 1
            if (bucketsList.get(INDEX_1).cursize == 0){
                // Move points to Bucket 1
                bucketsList.get(INDEX_1).points = bucketsList.get(INDEX_0).points;
                bucketsList.get(INDEX_1).cursize = MAX_BUCKET_SIZE;
                // Empty Bucket 0
                bucketsList.get(INDEX_0).points = new Point[MAX_BUCKET_SIZE];
                bucketsList.get(INDEX_0).cursize = 0;
            }
            else{
                // If Bucket 1 is full, move the points from Bucket 0 to Bucket 1 spillover
                bucketsList.get(INDEX_1).spillover = bucketsList.get(INDEX_0).points;
                // Empty Bucket 0
                bucketsList.get(INDEX_0).points = new Point[MAX_BUCKET_SIZE];
                bucketsList.get(INDEX_0).cursize = 0;
                curBucket++;
                nextBucket++;

                // If there is no next Bucket (i.e. Bucket 2), make a new one
                if (bucketsList.size() == nextBucket) {
                    bucketsList.add(new Bucket(nextBucket));
                }

                // As long as the next Bucket is full, place the produced coreset to the spillover of the next Bucket
                while(bucketsList.get(nextBucket).cursize == MAX_BUCKET_SIZE){

                    // Extract a coreset from curBucket.points & curBucket.spillover
                    bucketsList.get(nextBucket).spillover = TreeCoreset.unionTreeCoreset(bucketsList.get(curBucket).points,
                            bucketsList.get(curBucket).spillover, MAX_BUCKET_SIZE, d, random);

                    // Empty current Bucket points and spillover arrays
                    bucketsList.get(curBucket).points = null;
                    bucketsList.get(curBucket).spillover = null;
                    bucketsList.get(curBucket).cursize = 0;
                    curBucket++;
                    nextBucket++;

                    // If there is no next Bucket, make a new one
                    if (bucketsList.size() == nextBucket) {
                        bucketsList.add(new Bucket(nextBucket));
                    }
                }

                // Extract a coreset from curBucket.points & curBucket.spillover
                bucketsList.get(nextBucket).points = TreeCoreset.unionTreeCoreset(bucketsList.get(curBucket).points,
                        bucketsList.get(curBucket).spillover, MAX_BUCKET_SIZE, d, random);

                // Empty current Bucket points and spillover arrays
                bucketsList.get(curBucket).points = null;
                bucketsList.get(curBucket).spillover = null;
                bucketsList.get(curBucket).cursize = 0;

                // Set next Bucket size as maxBucketSize
                bucketsList.get(nextBucket).cursize = MAX_BUCKET_SIZE;
            }
        }

        // Put input point into Bucket 0
        bucketsList.get(INDEX_0).points[bucketsList.get(INDEX_0).cursize] = point;
        bucketsList.get(INDEX_0).cursize++;
    }

    /**
     * Extracts a coreset with MAX_BUCKET_SIZE points from the union of all
     * non-empty buckets (including Bucket 0) from the list of buckets.
     * @return  Returns the Point array with the weighted points of the produced coreset.
     */
    public Point[] getCoresetFromManager(){

        ArrayList<Point> weightedPoints = new ArrayList<>();

        for (Bucket b: bucketsList){
            if(b.cursize != 0){
                for(int i=0; i<b.cursize; i++){
                    weightedPoints.add(b.points[i]);
                }
            }
        }

        Point[] coreset = TreeCoreset.unionTreeCoreset(weightedPoints.toArray(new Point[weightedPoints.size()]),
                new Point[0], MAX_BUCKET_SIZE, d, random);

        return coreset;
    }

    public ArrayList<Bucket> getBucketsList() {
        return bucketsList;
    }

    public void setBucketsList(ArrayList<Bucket> bucketsList) {
        this.bucketsList = bucketsList;
    }

}