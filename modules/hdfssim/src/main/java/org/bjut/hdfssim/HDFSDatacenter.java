package org.bjut.hdfssim;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public class HDFSDatacenter extends SimEntity{

    public Namenode namenode;
    public Map<Integer,HDFSHost> hostList; // datanodeId, HDFSHost
    /** The last time some cloudlet was processed in the datacenter. */
    private double lastProcessTime;
    private DatanodeAllocationPolicy policy;

    public HDFSDatacenter(String name, Namenode namenode, DatanodeAllocationPolicy policy) {
        super(name);
        this.namenode = namenode;
        this.hostList = namenode.getHDFSHostList();
        this.policy = policy;
    }

    @Override
    public void startEntity() {
        int gisID = CloudSim.getCloudInfoServiceEntityId();
        sendNow(gisID, CloudSimTags.REGISTER_RESOURCE, getId());
    }

    @Override
    public void shutdownEntity() {

    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // 处理提交的Request
            case CloudSimTags.RequestCreate:
                processRequestCreate(ev);
                break;
        }
    }

    protected void processRequestCreate(SimEvent ev)
    {
        Request request = (Request)ev.getData();
        if(request.isFinished()) return;

        request.start(CloudSim.clock());
        Datanode datanode = policy.getDatanode(request.getCurrentReadCloudlet().getBlockList(),request.getAddr());
        datanode.getHost().addCloudLet(CloudSim.clock(),request.getCurrentReadCloudlet());
    }

    protected void updateCloudletProcessing()
    {

    }


    public double getLastProcessTime() {
        return lastProcessTime;
    }

    public void setLastProcessTime(double lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }
}
