package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Configuration;

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
        double tmpTime = nextStartTime + restLength / speed;
        if(Math.abs(time - tmpTime) < Configuration.getDoubleProperty("precision") || tmpTime < time)
        {
            finishedTime = nextStartTime + restLength / speed;
            restLength = 0;
            isFinished = true;
        }
        else
        {
             restLength -= t * speed;
             nextStartTime = time;
            predictTime = Double.MAX_VALUE;
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
