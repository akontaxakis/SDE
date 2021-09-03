package lib.WLSH;

public class HashedWindow {

    private String simulation;
    private int start_time;
    private int hammingWeight;
    private WLSH lsh_vector;

    public HashedWindow(String simulation, int start_time, int hammingWeight, WLSH lsh_vector) {
        this.simulation = simulation;
        this.start_time = start_time;
        this.hammingWeight = hammingWeight;
        this.lsh_vector = lsh_vector;
    }

    public String getSimulation() {
        return simulation;
    }

    public void setSimulation(String simulation) {
        this.simulation = simulation;
    }

    public int getStart_time() {
        return start_time;
    }

    public void setStart_time(int start_time) {
        this.start_time = start_time;
    }

    public int getHammingWeight() {
        return hammingWeight;
    }

    public void setHammingWeight(int hammingWeight) {
        this.hammingWeight = hammingWeight;
    }

    public WLSH getLsh_vector() {
        return lsh_vector;
    }

    public void setLsh_vector(WLSH lsh_vector) {
        this.lsh_vector = lsh_vector;
    }

    @Override
    public String toString() {
        return "HashedWindow{" +
                "simulation='" + simulation + '\'' +
                ", start_time=" + start_time +
                ", hammingWeight=" + hammingWeight +
                ", lsh_vector=" + lsh_vector +
                '}';
    }
}
