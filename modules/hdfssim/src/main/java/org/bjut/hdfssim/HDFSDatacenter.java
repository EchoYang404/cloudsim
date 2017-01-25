package org.bjut.hdfssim;

import java.util.*;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Migrationer;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.HCloudlet;
import org.bjut.hdfssim.models.Request.MigrateCloudlet;
import org.bjut.hdfssim.models.Request.ReadCloudlet;
import org.bjut.hdfssim.models.Request.Request;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import javax.xml.crypto.Data;

public class HDFSDatacenter extends SimEntity {

    private Namenode namenode;
    private Map<Integer, HDFSHost> hostList; // datanodeId, HDFSHost
    private DatanodeAllocationPolicy policy;
    private Migrationer migrationer;
    private boolean isMigrate;

    public HDFSDatacenter(String name, Namenode namenode, DatanodeAllocationPolicy policy, boolean isMigrate) {
        super(name);
        this.namenode = namenode;
        this.hostList = namenode.getHDFSHostList();
        this.policy = policy;
        this.migrationer = new Migrationer(namenode);
        this.isMigrate = isMigrate;
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
            case CloudSimTags.CloudletExcute:
                processCloudletExcute(ev);
                updateCloudletProcessing();
                break;
        }
    }

    protected void processRequestCreate(SimEvent ev) {
        Request request = (Request) ev.getData();
        if (request.isFinished()) return;

        request.start(CloudSim.clock());
        allocateDatanode(request.getCurrentReadCloudlet(), request.getAddr());
        send(getId(), 0, CloudSimTags.CloudletExcute, request.getCurrentReadCloudlet());
    }


    private void allocateDatanode(HCloudlet cloudlet, Datanode addr) {
        Datanode datanode = policy.getDatanode(cloudlet.getBlockList(), addr);
        datanode.getHost().addCloudLet(CloudSim.clock(), cloudlet);
    }

    protected void updateCloudletProcessing() {
        HCloudlet result = null;
        double minTime = Double.MAX_VALUE;

        Iterator<HDFSHost> iterator = hostList.values().iterator();
        while (iterator.hasNext()) {
            HDFSHost host = iterator.next();
            // 尝试执行所有任务，找到当前阶段（CPU,Disk,BW,NET）执行时间最小的任务
            HCloudlet tmp = host.tryExcuteCloudlets();
            if (tmp != null && tmp.getCurrentStage().getPredictTime() < minTime) {
                result = tmp;
                minTime = tmp.getCurrentStage().getPredictTime();
            }
        }
        if (result != null) {
            double delay = result.getCurrentStage().getPredictTime() - CloudSim.clock();
            send(getId(), delay, CloudSimTags.CloudletExcute, result);
        }
    }

    protected void checkMigartion(double time) {
        Iterator<MigrateCloudlet> iterator = migrationer.check(time).iterator();
        while (iterator.hasNext()) {
            MigrateCloudlet mc = iterator.next();
            mc.start(CloudSim.clock());
            allocateDatanode(mc, mc.getDestNode());
        }
    }

    protected void processCloudletExcute(SimEvent ev) {
        if (isMigrate) {
            checkMigartion(ev.eventTime());
        }
        //Log.printLine("time " + ev.eventTime());
        //Log.printLine(result.getRequest().getId() + " " + result.getRequest().getCurrentNum() + " " + result
        // .getRequest().getCurrentReadCloudlet().getCurrentStageType() + " " + result.getCurrentStage()
        // .getPredictTime());
        Iterator<HDFSHost> iterator = hostList.values().iterator();
        SortedSet<HCloudlet> completeList = new TreeSet<>();
        while (iterator.hasNext()) {
            HDFSHost host = iterator.next();
            // 执行time之前的所有任务
            SortedSet<HCloudlet> tmp = host.excuteCloudlets(ev.eventTime());
            completeList.addAll(tmp);
        }
        Iterator<HCloudlet> cloudletIterator = completeList.iterator();
        while (cloudletIterator.hasNext()) {
            HCloudlet c = cloudletIterator.next();
            if (c.getClass() == ReadCloudlet.class) {
                Request r = ((ReadCloudlet) c).getRequest();
                if (r.toNext()) {
                    allocateDatanode(r.getCurrentReadCloudlet(), r.getAddr());
                    //send(getId(), 0, CloudSimTags.CloudletExcute, r.getCurrentReadCloudlet());
                }
            }
        }
    }

    public Migrationer getMigrationer() {
        return migrationer;
    }
}
