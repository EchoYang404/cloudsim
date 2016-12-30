package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.util.Id;
import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.UtilizationModelFull;

public class Task extends Cloudlet {

    public String name;
    public int requestId;
    public boolean isFinished;
    public int mi;
    public int blockNum;

    public Task(String name, int blockNum, int mi)
    {
        super(Id.pollId(Task.class), 0, 1, 0, 0, new UtilizationModelFull(), new UtilizationModelFull(),
                new UtilizationModelFull());
        this.name = name;
        requestId = -1;
        isFinished = false;
        this.mi = mi;
        this.blockNum = blockNum;

        //this.setUserId(Cloud.brokerID);
    }
}
