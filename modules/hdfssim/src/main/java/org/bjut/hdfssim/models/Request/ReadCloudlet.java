package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public class ReadCloudlet implements Comparable<ReadCloudlet>, Cloneable {
    private int blockId;
    private List<Block> blockList;
    private Request request;

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


    public ReadCloudlet(int blockId, List<Block> blockList, Request request) {
        this.blockId = blockId;
        this.blockList = blockList;
        this.cpuStage = new Stage(Configuration.getIntProperty("readBlockMips"));
        this.diskStage = new Stage(blockList.get(0).getSize());
        this.bwStage = new Stage(blockList.get(0).getSize());
        this.maxStage = ReadCloudlet.BW;
        this.netStage = null;

        this.request = request;
        this.startTime = Double.MAX_VALUE;
        this.finishedTime = Double.MAX_VALUE;
        this.currentStage = ReadCloudlet.CPU;
        this.host = null;
        this.isStarted = false;
        this.isFinished = false;
    }

    public List<Block> getBlockList() {
        return blockList;
    }

    public int getBlockId() {
        return blockId;
    }

    public void start(double startTime) {
        this.startTime = startTime;
        this.isStarted = true;
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

    public void setHost(HDFSHost host) {

        this.host = host;
        if (this.host.getDatanode().getRackId() == this.request.getAddr().getRackId()) {
            this.maxStage = ReadCloudlet.NET;
            this.netStage = new Stage(blockList.get(0).getSize());
        }
    }

    public HDFSHost getHost() {
        return host;
    }

    public double getFinishedTime() {
        return finishedTime;
    }

    @Override
    public int compareTo(ReadCloudlet o) {
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

    @Override
    public ReadCloudlet clone() {

        ReadCloudlet cloudlet = null;
        try {
            cloudlet = (ReadCloudlet) super.clone();
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return cloudlet;
    }

    public int getMaxStage() {
        return maxStage;
    }

    public Request getRequest() {
        return request;
    }
}
