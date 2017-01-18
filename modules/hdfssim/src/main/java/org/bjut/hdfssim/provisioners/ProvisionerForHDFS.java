package org.bjut.hdfssim.provisioners;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.models.Request.HCloudlet;
import org.bjut.hdfssim.models.Request.ReadCloudlet;

import java.io.Serializable;
import java.util.*;

public class ProvisionerForHDFS implements Serializable {
    private double capacity;
    private int currentNum;
    private final int type;
    // 正在运行的任务列表
    protected List<HCloudlet> cloudletList;

    public ProvisionerForHDFS(double capacity, int type) {
        this.cloudletList = new ArrayList<>();
        currentNum = 0;
        this.capacity = capacity;
        this.type = type;
    }

    private double getSpeed() {
        if(currentNum == 0) return capacity;
        return capacity / currentNum;
    }

    public void addCloudlet(HCloudlet cloudlet, double time) {
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

    public SortedSet<HCloudlet> excuteCloudlets(double time) {
        SortedSet<HCloudlet> finished = new TreeSet<>();
        if(cloudletList.isEmpty()) return finished;
        Iterator<HCloudlet> iterator = cloudletList.iterator();
        double speed = getSpeed();
        while (iterator.hasNext()) {
            HCloudlet cloudlet = iterator.next();

            cloudlet.getCurrentStage().excute(time, speed);
            if (cloudlet.getCurrentStage().isFinished()) {
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

    public HCloudlet tryExcute()
    {
        double time = Double.MAX_VALUE;
        double speed = getSpeed();
        HCloudlet result = null;
        for(HCloudlet cloudlet : this.cloudletList)
        {
            double preTime = cloudlet.getCurrentStage().getPredictTime();
            cloudlet.getCurrentStage().tryExcute(speed);
            double newTime = cloudlet.getCurrentStage().getPredictTime();
            if(Math.abs(preTime - newTime) < Configuration.getDoubleProperty("precision"))
            {
                continue;
            }
            if(time + Configuration.getDoubleProperty("precision") > newTime)
            {
                time = newTime;
                result = cloudlet;
            }
        }
        return result;
    }

    public double getCapacity() {
        return capacity;
    }
}
