package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDFSBroker;
import org.bjut.hdfssim.HFile;
import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.util.Id;
import org.cloudbus.cloudsim.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Request {
    private int id;
    private List<ReadCloudlet> cloudletList;
    private HFile hFile;
    private double submitTime;
    private HDFSBroker broker;

    // source datanode to read file
    private Datanode addr;

    private double finishedTime;
    private boolean isFinished;
    private int currentNum;

    public Request(Datanode addr, HFile hFile, double submitTime) {
        this.id = Id.pollId(this.getClass());
        this.hFile = hFile;
        this.submitTime = submitTime;
        this.addr = addr;
        this.cloudletList = new ArrayList<>();
        this.finishedTime = Double.MAX_VALUE;
        this.isFinished = false;
        this.currentNum = 0;

        createReadCloudlet();
    }

    private void createReadCloudlet() {
        Iterator<Map.Entry<Integer, List<Block>>> entries = this.hFile.getBlockList().entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Integer, List<Block>> entry = entries.next();
            this.cloudletList.add(new ReadCloudlet(entry.getKey(), entry.getValue(), this));
        }
    }

    public ReadCloudlet getCurrentReadCloudlet() {
        return cloudletList.get(currentNum);
    }

    public boolean isFinished() {
        return isFinished;
    }

    public double getSubmitTime() {
        return submitTime;
    }

    public Datanode getAddr() {
        return addr;
    }

    public void start(double time) {
        this.getCurrentReadCloudlet().start(time);
    }

    public HDFSBroker getBroker() {
        return broker;
    }

    public void setBroker(HDFSBroker broker) {
        this.broker = broker;
    }

    public boolean toNext() {
        if (this.cloudletList.size() > (currentNum + 1))
        {
            double startTime = this.cloudletList.get(currentNum).getFinishedTime();
            this.currentNum++;
            this.getCurrentReadCloudlet().start(startTime);
            return true;
        }
        else
        {
            this.isFinished = true;
            this.finishedTime = this.getCurrentReadCloudlet().getFinishedTime();
            Log.printLine("request " + id + " time " + finishedTime);
            return false;
        }
    }
    public HFile gethFile() {
        return hFile;
    }

    public double getFinishedTime() {
        return finishedTime;
    }

    public int getCurrentNum() {
        return currentNum;
    }

    public int getId() {
        return id;
    }
}
