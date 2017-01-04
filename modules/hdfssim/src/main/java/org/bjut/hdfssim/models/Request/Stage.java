package org.bjut.hdfssim.models.Request;

public class Stage {
    private double length;
    private double restLength;

    private double nextStartTime = Double.MAX_VALUE;
    private double finishedTime = Double.MAX_VALUE;
    private double predictTime = Double.MAX_VALUE;

    private boolean isFinished = false;
    public Stage(double length)
    {
        this.length = length;
        this.restLength = length;
    }

    public void start(double time)
    {
        this.nextStartTime = time;
    }

    public void excute(double time, double speed)
    {
        if(isFinished) return;

        double t = time - nextStartTime;
        if(t * speed >= restLength || restLength < 0.1)
        {
            finishedTime = nextStartTime + restLength / speed;
            restLength = 0;
            isFinished = true;
        }
        else
        {
             restLength -= t * speed;
             nextStartTime = time;
        }
    }

    public boolean isFinished() {
        return isFinished;
    }

    public double getFinishedTime() {
        return finishedTime;
    }

    public double getNextStartTime() {
        return nextStartTime;
    }

    public void tryExcute(double speed)
    {
        if(isFinished) return;
        predictTime = nextStartTime + restLength / speed;
    }

    public double getPredictTime() {
        return predictTime;
    }
}
