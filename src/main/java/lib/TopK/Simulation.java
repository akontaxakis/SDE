package lib.TopK;

import java.util.HashMap;

public class Simulation {

    private String pid;
    private int alive;

    private int path;
    private HashMap<Integer, Integer> data = new HashMap<>();
    private int countDown;
    private int downtrend;
    private int currtime;
    private boolean topk;

    public Simulation(int cd, String ppid, int al, int t, boolean f) {
        pid=ppid;
        alive = al;
        countDown =cd;
        currtime = t;
        topk = f;
        data.put(t,al);
    }

    public void updateData(int t, int d){
        setCurrtime(t);
        setAlive(d);
        data.put(t,d);
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
