package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;
import org.bjut.hdfssim.models.HDFS.Datanode;

import java.util.List;

public class ReadCloudlet implements Comparable<ReadCloudlet> {
    private int blockId;
    private List<Block> blockList;
    private Request request;

    //初始时未选择执行的host
    private HDFSHost host = null;
    private double startTime = -1;
    private double finishedTime = -1;

    private boolean isStarted = false;
    private boolean isFinished = false;

    private Stage cpuStage;
    private Stage diskStage;
    private Stage bwStage;

    private int currentStage = ReadCloudlet.CPU;

    public static final int CPU = 1;
    public static final int DISK = 2;
    public static final int BW = 3;


    public ReadCloudlet(int blockId, List<Block> blockList, Request request) {
        this.blockId = blockId;
        this.blockList = blockList;
        this.cpuStage = new Stage(Configuration.getIntProperty("readBlockMips"));
        this.diskStage = new Stage(blockList.get(0).getSize());
        this.bwStage = new Stage(blockList.get(0).getSize());
        this.request = request;
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
                return bwStage;
        }
        return null;
    }

    public int getCurrentStageType()
    {
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
                currentStage = -1;
                break;
        }
    }

    public void setHost(HDFSHost host) {
        this.host = host;
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
            thisTime = this.getCurrentStage().getStartTime();
        }
        if (o.getCurrentStage() == null) {
            oTime = o.getFinishedTime();
        } else {
            oTime = o.getCurrentStage().getStartTime();
        }

        if (thisTime == oTime) {
            return 0;
        } else if (thisTime < oTime) {
            return -1;
        }

        return 1;
    }
}
