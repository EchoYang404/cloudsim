package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HFile;
import org.bjut.hdfssim.models.HDFS.Datanode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Request {
    private List<ReadCloudlet> cloudletList;
    private HFile hFile;
    private double submitTime;

    // source datanode to read file
    private Datanode addr;
    private int current;

    private double startTime = -1;
    private double finishedTime = -1;
    private boolean isStarted = false;
    private boolean isFinished = false;
    private int currentCloudlet = 0;

    public Request(Datanode addr, HFile hFile, double submitTime)
    {
        this.hFile = hFile;
        this.submitTime = submitTime;
        this.addr = addr;
        this.cloudletList = new ArrayList<>();
        createReadCloudlet();
    }

    private void createReadCloudlet()
    {
        Iterator<Map.Entry<Integer, List<Block>>> entries = this.hFile.getBlockList().entrySet().iterator();
        while(entries.hasNext())
        {
            Map.Entry<Integer, List<Block>> entry = entries.next();
            this.cloudletList.add(new ReadCloudlet(entry.getKey(), entry.getValue(), this));
        }

    }

    public ReadCloudlet getCurrentReadCloudlet()
    {
        return cloudletList.get(currentCloudlet);
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

    public void start(double time)
    {
        this.startTime = time;
        this.getCurrentReadCloudlet().start(time);
        this.isStarted = true;
    }

    // TODO 执行cloudlet
}
