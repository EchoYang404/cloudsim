package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public abstract class HCloudlet implements Comparable<HCloudlet> {
    private int blockId;
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

    public HCloudlet(int blockId, double size) {
        this.blockId = blockId;

        this.cpuStage = new Stage(Configuration.getIntProperty("readBlockMips"));
        this.diskStage = new Stage(size);
        this.bwStage = new Stage(size);
        this.maxStage = HCloudlet.BW;
        this.netStage = null;

        this.startTime = Double.MAX_VALUE;
        this.finishedTime = Double.MAX_VALUE;
        this.currentStage = HCloudlet.CPU;
        this.host = null;
        this.isStarted = false;
        this.isFinished = false;
    }

    public Stage getCurrentStage() {
        switch (currentStage) {
            case HCloudlet.CPU:
                return cpuStage;
            case HCloudlet.DISK:
                return diskStage;
            case HCloudlet.BW:
                if (bwStage.isFinished() && this.maxStage == HCloudlet.BW) {
                    this.isFinished = true;
                    this.finishedTime = bwStage.getFinishedTime();
                }
                return bwStage;
            case HCloudlet.NET:
                if (netStage.isFinished() && this.maxStage == HCloudlet.NET) {
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
            case HCloudlet.CPU:
                currentStage = HCloudlet.DISK;
                break;
            case HCloudlet.DISK:
                currentStage = HCloudlet.BW;
                break;
            case HCloudlet.BW:
                if (maxStage == HCloudlet.BW) {
                    currentStage = -1;
                } else {
                    currentStage = HCloudlet.NET;
                }
                break;
            case HCloudlet.NET:
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

    public int getBlockId() {
        return blockId;
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
