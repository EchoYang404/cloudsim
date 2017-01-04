package org.bjut.hdfssim.models.Request;

public class Stage {
    private double length;
    private double restLength;

    private double startTime = -1;
    private double finishedTime = -1;

    private boolean isFinished = false;

    public Stage(double length)
    {
        this.length = length;
        this.restLength = length;
    }

    public void start(double time)
    {
        this.startTime = time;
    }

    public void excute(double time, double speed)
    {
        if(isFinished) return;

        double t = time - startTime;
        if(t * speed > restLength)
        {
            finishedTime = startTime + restLength / speed;
            restLength = 0;
            isFinished = true;
        }
        else
        {
             restLength -= t * speed;
             startTime = time;
        }
    }

    public boolean isFinished() {
        return isFinished;
    }

    public double getFinishedTime() {
        return finishedTime;
    }

    public double getStartTime() {
        return startTime;
    }
}
