package org.bjut.hdfssim.provisioners;

import org.bjut.hdfssim.models.Request.ReadCloudlet;

import java.io.Serializable;
import java.util.*;

public class ProvisionerForHDFS implements Serializable {
    private double capacity;

    private int currentNum;
    private final int type;
    // 正在运行的任务列表
    protected List<ReadCloudlet> cloudletList;

    public ProvisionerForHDFS(double capacity, int type) {
        this.cloudletList = new ArrayList<>();
        currentNum = 0;
        this.capacity = capacity;
        this.type = type;
    }

    private double getSpeed() {
        return capacity / currentNum;
    }

    public void addCloudlet(ReadCloudlet cloudlet, double time) {
        if (cloudlet.getCurrentStageType() != this.type) {
            try {
                throw new Exception("Type of Provisoner and cloudlet is different!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        this.cloudletList.add(cloudlet);
        cloudlet.getCurrentStage().start(time);
        this.currentNum++;
    }

    public SortedSet<ReadCloudlet> excuteCloudlets(double time) {
        SortedSet<ReadCloudlet> finished = new TreeSet<>();
        Iterator<ReadCloudlet> iterator = cloudletList.iterator();
        double speed = getSpeed();
        while (iterator.hasNext()) {
            ReadCloudlet cloudlet = iterator.next();

            cloudlet.getCurrentStage().excute(time, speed);

            double finishedTime = cloudlet.getCurrentStage().getFinishedTime();
            if (finishedTime <= time) {
                finished.add(cloudlet);
                iterator.remove();
                currentNum--;
            }
        }
        return finished;
    }

    public int getCurrentNum() {
        return currentNum;
    }

    public ReadCloudlet tryExcute()
    {
        double time = Double.MAX_VALUE;
        double speed = getSpeed();
        ReadCloudlet result = null;
        for(ReadCloudlet cloudlet : this.cloudletList)
        {
            cloudlet.getCurrentStage().tryExcute(speed);
            if(time > cloudlet.getCurrentStage().getPredictTime())
            {
                time = cloudlet.getCurrentStage().getPredictTime();
                result = cloudlet;
            }
        }
        return result;
    }
}
