package TopK;

import java.util.HashMap;

public class Simulation {

    private String pid;
    private int alive;

    private int path;
    private HashMap<Integer, Integer> data;
    private int n_alive;
    private int countDown;
    private int downtrend;
    private int currtime;
    private boolean topk;

    public Simulation(int cd, String ppid,int n_alive, int al, int t, boolean f) {
        pid=ppid;
        alive = al;
        countDown =cd;
        currtime = t;
        topk = f;
        data.put(t,al/n_alive);
    }

    public void updateData(int t, int d){
        setCurrtime(t);
        setAlive(d/n_alive);
        data.put(t,d/n_alive);
    }


    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAlive() {
        return alive;
    }

    public void setAlive(int alive) {
        this.alive = alive;
    }

    public int getPath() {
        return path;
    }

    public void setPath(int path) {
        this.path = path;
    }

    public HashMap getData() {
        return data;
    }

    public void setData(HashMap<Integer, Integer> data) {
        this.data = data;
    }

    public int getN_alive() {
        return n_alive;
    }

    public void setN_alive(int n_alive) {
        this.n_alive = n_alive;
    }

    public int getCountDown() {
        return countDown;
    }

    public void setCountDown(int countDown) {
        this.countDown = countDown;
    }

    public int getDowntrend() {
        return downtrend;
    }

    public void setDowntrend(int downtrend) {
        this.downtrend = downtrend;
    }

    public int getCurrtime() {
        return currtime;
    }

    public void setCurrtime(int currtime) {
        this.currtime = currtime;
    }

    public boolean isTopk() {
        return topk;
    }

    public void setTopk(boolean topk) {
        this.topk = topk;
    }

    public int getAlive(int time) {
        return data.get(time);
    }
}
