package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public abstract class HCloudlet implements Comparable<HCloudlet> {
    //初始时未选择执行的host
    private HDFSHost host;
    private double startTime;
    private double finishedTime;

    private boolean isStarted;
    private boolean isFinished;

    private Stage cpuStage;
    private Stage diskStage;
    private Stage bwStage;
    private Stage netStage;

    private int currentStage;
    private int maxStage;
    public static final int CPU = 1;
    public static final int DISK = 2;
    public static final int BW = 3;
    public static final int NET = 4;

    public HCloudlet(double size) {
        this.cpuStage = new Stage(Configuration.getIntProperty("readBlockMips"));
        this.diskStage = new Stage(size);
        this.bwStage = new Stage(size);
        this.maxStage = ReadCloudlet.BW;
        this.netStage = null;

        this.startTime = Double.MAX_VALUE;
        this.finishedTime = Double.MAX_VALUE;
        this.currentStage = ReadCloudlet.CPU;
        this.host = null;
        this.isStarted = false;
        this.isFinished = false;
    }

    public Stage getCurrentStage() {
        switch (currentStage) {
            case ReadCloudlet.CPU:
                return cpuStage;
            case ReadCloudlet.DISK:
                return diskStage;
            case ReadCloudlet.BW:
                if (bwStage.isFinished() && this.maxStage == ReadCloudlet.BW) {
                    this.isFinished = true;
                    this.finishedTime = bwStage.getFinishedTime();
                }
                return bwStage;
            case ReadCloudlet.NET:
                if (netStage.isFinished() && this.maxStage == ReadCloudlet.NET) {
                    this.isFinished = true;
                    this.finishedTime = netStage.getFinishedTime();
                }
                return netStage;
        }
        return null;
    }

    public int getCurrentStageType() {
        return currentStage;
    }

    public void toNextStage() {
        switch (currentStage) {
            case ReadCloudlet.CPU:
                currentStage = ReadCloudlet.DISK;
                break;
            case ReadCloudlet.DISK:
                currentStage = ReadCloudlet.BW;
                break;
            case ReadCloudlet.BW:
                if (maxStage == ReadCloudlet.BW) {
                    currentStage = -1;
                } else {
                    currentStage = ReadCloudlet.NET;
                }
                break;
            case ReadCloudlet.NET:
                currentStage = -1;
                break;
        }
    }

    public abstract void allocateHost(HDFSHost host);

    public HDFSHost getHost() {
        return host;
    }

    public double getFinishedTime() {
        return finishedTime;
    }

    public int getMaxStage() {
        return maxStage;
    }

    public void setMaxStage(int maxStage) {
        this.maxStage = maxStage;
    }

    public void setNetStage(Stage netStage) {
        this.netStage = netStage;
    }

    public void setHost(HDFSHost host) {
        this.host = host;
    }

    public void setStartTime(double startTime) {
        this.startTime = startTime;
    }

    public void setStarted(boolean started) {
        isStarted = started;
    }

    public void start(double startTime) {
        this.setStartTime(startTime);
        this.setStarted(true);
    }

    @Override
    public int compareTo(HCloudlet o) {
        double thisTime, oTime;
        if (this.getCurrentStage() == null) {
            thisTime = this.getFinishedTime();
        } else {
            thisTime = this.getCurrentStage().getNextStartTime();
        }
        if (o.getCurrentStage() == null) {
            oTime = o.getFinishedTime();
        } else {
            oTime = o.getCurrentStage().getNextStartTime();
        }

        if (thisTime == oTime) {
            return 0;
        } else if (thisTime < oTime) {
            return -1;
        }
        return 1;
    }
}
