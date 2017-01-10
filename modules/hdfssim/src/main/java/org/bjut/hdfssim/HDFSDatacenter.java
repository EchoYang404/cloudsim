package org.bjut.hdfssim;

import java.util.*;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.ReadCloudlet;
import org.bjut.hdfssim.models.Request.Request;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

public class HDFSDatacenter extends SimEntity {

    public Namenode namenode;
    public Map<Integer, HDFSHost> hostList; // datanodeId, HDFSHost
    /**
     * The last time some cloudlet was processed in the datacenter.
     */
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
                //updateCloudletProcessing();
                break;

            case CloudSimTags.RequestExcute:
                processRequestExcute(ev);
                updateCloudletProcessing();
                break;
        }
    }

    protected void processRequestCreate(SimEvent ev) {
        Request request = (Request) ev.getData();
        if (request.isFinished()) return;

        request.start(CloudSim.clock());
        allocateDatanode(request);
        send(getId(), 0, CloudSimTags.RequestExcute, request.getCurrentReadCloudlet());
    }

    private void allocateDatanode(Request request) {
        Datanode datanode = policy.getDatanode(request.getCurrentReadCloudlet().getBlockList(), request.getAddr());
        datanode.getHost().addCloudLet(CloudSim.clock(), request.getCurrentReadCloudlet());
    }

    protected void updateCloudletProcessing() {
        ReadCloudlet result = null;
        double time = Double.MAX_VALUE;

        Iterator<HDFSHost> iterator = hostList.values().iterator();
        while (iterator.hasNext()) {
            HDFSHost host = iterator.next();
            // 尝试执行所有任务，找到当前阶段（CPU,Disk,BW,NET）执行时间最小的任务
            ReadCloudlet tmp = host.tryExcuteCloudlets();
            if (tmp != null && tmp.getCurrentStage().getPredictTime() < time) {
                result = tmp;
                time = tmp.getCurrentStage().getPredictTime();
            }
        }
        if (result != null) {
            double delay = result.getCurrentStage().getPredictTime() - CloudSim.clock();
            send(getId(), delay, CloudSimTags.RequestExcute, result);
        }
    }

    protected void processRequestExcute(SimEvent ev) {
        //Log.printLine("time " + ev.eventTime());
        ReadCloudlet result = (ReadCloudlet) ev.getData();
        //Log.printLine(result.getRequest().getId() + " " + result.getRequest().getCurrentCloudlet() + " " + result.getRequest().getCurrentReadCloudlet().getCurrentStageType() + " " + result.getCurrentStage().getPredictTime());
        Iterator<HDFSHost> iterator = hostList.values().iterator();
        SortedSet<ReadCloudlet> completeList = new TreeSet<>();
        while (iterator.hasNext()) {
            HDFSHost host = iterator.next();
            // 执行time之前的所有任务
            completeList.addAll(host.excuteCloudlets(ev.eventTime()));
        }
        Iterator<ReadCloudlet> cloudletIterator = completeList.iterator();
        while (cloudletIterator.hasNext()) {
            ReadCloudlet c = cloudletIterator.next();
            if (c.getRequest().toNext()) {
                allocateDatanode(c.getRequest());
            }
        }
    }


    public double getLastProcessTime() {
        return lastProcessTime;
    }

    public void setLastProcessTime(double lastProcessTime) {
        this.lastProcessTime = lastProcessTime;
    }
}
