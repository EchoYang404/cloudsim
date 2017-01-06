package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.Request.ReadCloudlet;
import org.bjut.hdfssim.provisioners.ProvisionerForHDFS;

import java.io.*;
import java.util.*;

public class HDFSHost implements Serializable {
    /**
     * The id of the HDFSHost.
     */
    private int id;

    private Datanode datanode;

    // TODO 在执行的cloudlet列表
    private List<ReadCloudlet> cloudletList;

    // TODO 已完成的cloudlet列表
    private List<ReadCloudlet> finishedList;

    /**
     * The ssd provisioner for hdfs.
     */
    private ProvisionerForHDFS ssdProvisioner;

    /**
     * The hdd provisioner for hdfs.
     */
    private ProvisionerForHDFS hddProvisioner;

    /**
     * The bw provisioner for hdfs.
     */
    private ProvisionerForHDFS bwProvisioner;

    /**
     * The net provisioner for hdfs.
     */
    private ProvisionerForHDFS netProvisioner;

    private List<ProvisionerForHDFS> peProvisionerList;

    public HDFSHost(Datanode datanode, double ssdMaxTransferRate, double hddMaxTransferRate, double bw, int coreNum, double mips) {
        this.id = datanode.getId();
        this.datanode = datanode;
        this.ssdProvisioner = new ProvisionerForHDFS(ssdMaxTransferRate, ReadCloudlet.DISK);
        this.hddProvisioner = new ProvisionerForHDFS(hddMaxTransferRate, ReadCloudlet.DISK);
        this.bwProvisioner = new ProvisionerForHDFS(bw, ReadCloudlet.BW);
        this.netProvisioner = new ProvisionerForHDFS(Configuration.getDoubleProperty("remoteRackTransferRate"), ReadCloudlet.NET);
        this.peProvisionerList = new ArrayList<>();
        for (int i = 0; i < coreNum; i++) {
            peProvisionerList.add(new ProvisionerForHDFS(mips, ReadCloudlet.CPU));
        }
        this.finishedList = new ArrayList<>();
    }

    public void addCloudLet(double time, ReadCloudlet cloudlet) {
        cloudlet.setHost(this);
        int minCPUNum = Integer.MAX_VALUE;
        ProvisionerForHDFS pe = null;
        for (ProvisionerForHDFS provisioner : peProvisionerList) {
            if (provisioner.getCurrentNum() < minCPUNum) {
                pe = provisioner;
                minCPUNum = provisioner.getCurrentNum();
            }
        }
        pe.addCloudlet(cloudlet, time);
    }

    public SortedSet<ReadCloudlet> excuteCloudlets(double time) {

        SortedSet<ReadCloudlet> completeList = new TreeSet<>();
        // excute all cpus cloudlets
        for (ProvisionerForHDFS provisioner : peProvisionerList) {
            completeList.addAll(provisioner.excuteCloudlets(time));
        }
        Iterator<ReadCloudlet> iterator = completeList.iterator();
        while (iterator.hasNext()) {
            ReadCloudlet c = iterator.next();
            excuteFinishedCPUCloudlet(c);
            completeList.remove(c);
        }
        // excute all disk cloudlets
        completeList.addAll(ssdProvisioner.excuteCloudlets(time));
        completeList.addAll(hddProvisioner.excuteCloudlets(time));
        iterator = completeList.iterator();
        while (iterator.hasNext()) {
            ReadCloudlet c = iterator.next();
            excuteFinishedDiskCloudlet(c);
            completeList.remove(c);
        }

        // excute all bw cloudlets
        completeList.addAll(bwProvisioner.excuteCloudlets(time));

        iterator = completeList.iterator();
        while (iterator.hasNext()) {
            ReadCloudlet c = iterator.next();
            if (c.getMaxStage() == ReadCloudlet.NET) {
                excuteFinishedBwCloudlet(c);
                completeList.remove(c);
            }
        }

        completeList.addAll(netProvisioner.excuteCloudlets(time));
        completeList.addAll(this.finishedList);
        this.finishedList.clear();
        return completeList;
    }

    private void excuteFinishedCPUCloudlet(ReadCloudlet cloudlet) {
        // 下一阶段的开始时间
        double startTime = cloudlet.getCurrentStage().getFinishedTime();
        // block所在磁盘介质类型
        int type = cloudlet.getHost().datanode.getStorageTypeByBlockId(cloudlet.getBlockId());
        ProvisionerForHDFS provisioner;
        if (type == Storage.SSD) {
            provisioner = ssdProvisioner;
        } else {
            provisioner = hddProvisioner;
        }

        // 下一阶段开始前，执行所有任务
        SortedSet<ReadCloudlet> finishedDiskList = provisioner.excuteCloudlets(startTime);
        // 对下一阶段开始前完成的所有任务执行磁盘任务
        for (ReadCloudlet c : finishedDiskList) {
            excuteFinishedDiskCloudlet(c);
        }

        // 任务跳转至下下一阶段，并添加至provisioner中
        cloudlet.toNextStage();
        cloudlet.getCurrentStage().start(startTime);
        provisioner.addCloudlet(cloudlet, startTime);


    }

    private void excuteFinishedDiskCloudlet(ReadCloudlet cloudlet) {
        // 下一阶段的开始时间
        double startTime = cloudlet.getCurrentStage().getFinishedTime();
        // 下一阶段开始前，执行所有任务
        SortedSet<ReadCloudlet> finishedBwList = bwProvisioner.excuteCloudlets(startTime);
        for (ReadCloudlet c : finishedBwList) {
            excuteFinishedBwCloudlet(c);
        }

        // 任务跳转至下下一阶段，并添加至provisioner中
        cloudlet.toNextStage();
        cloudlet.getCurrentStage().start(startTime);
        bwProvisioner.addCloudlet(cloudlet, startTime);

    }

    private void excuteFinishedBwCloudlet(ReadCloudlet cloudlet) {
        // 下一阶段的开始时间
        double startTime = cloudlet.getCurrentStage().getFinishedTime();
        // 下一阶段开始前，执行所有任务
        finishedList.addAll(netProvisioner.excuteCloudlets(startTime));

        // 任务跳转至下下一阶段，并添加至provisioner中
        cloudlet.toNextStage();
        cloudlet.getCurrentStage().start(startTime);
        netProvisioner.addCloudlet(cloudlet, startTime);
    }


    public ReadCloudlet tryExcuteCloudlets() {
        ReadCloudlet result = null;
        for (ProvisionerForHDFS p : peProvisionerList) {
            result = tryExcute(p, result);
        }
        result = tryExcute(ssdProvisioner, result);
        result = tryExcute(hddProvisioner, result);
        result = tryExcute(bwProvisioner, result);
        result = tryExcute(netProvisioner, result);

        return result;
    }

    private ReadCloudlet tryExcute(ProvisionerForHDFS p, ReadCloudlet result) {
        double time;
        if (result == null) {
            time = Double.MAX_VALUE;
        } else {
            time = result.getCurrentStage().getPredictTime();
        }
        ReadCloudlet tmp = p.tryExcute();
        if (tmp != null && tmp.getCurrentStage().getPredictTime() < time) {
            result = tmp;
        }
        return result;
    }

    public Datanode getDatanode() {
        return datanode;
    }

    // 返回未使用的CPU核数
    public int getCpuUtilization() {
        Iterator<ProvisionerForHDFS> iterator = peProvisionerList.iterator();
        int num = 0;
        while (iterator.hasNext()) {
            int tmp = iterator.next().getCurrentNum();
            if (tmp == 0) {
                num++;
            }
        }
        return num / peProvisionerList.size();
    }

    // 返回block所在存储介质的当前速度
    public double getDiskUtilization(Block block) {
        if (datanode.getStorageTypeByBlockId(block.getId()) == Storage.HDD) {
            if (hddProvisioner.getCurrentNum() == 0) {
                return hddProvisioner.getCapacity();
            }
            return hddProvisioner.getCapacity() / hddProvisioner.getCurrentNum();
        }
        if (ssdProvisioner.getCurrentNum() == 0) {
            return ssdProvisioner.getCapacity();
        }
        return ssdProvisioner.getCapacity() / ssdProvisioner.getCurrentNum();
    }

    // 返回节点带宽的当前速度
    public double getBwUtilization() {
        if (bwProvisioner.getCurrentNum() == 0) {
            return bwProvisioner.getCapacity();
        }
        return bwProvisioner.getCapacity() / bwProvisioner.getCurrentNum();
    }

    // 返回网络带宽的当前速度
    public double getNetUtilization() {
        if (bwProvisioner.getCurrentNum() == 0) {
            return ssdProvisioner.getCapacity();
        }
        return netProvisioner.getCapacity() / bwProvisioner.getCurrentNum();
    }

    public double getBw()
    {
        return this.bwProvisioner.getCapacity();
    }

    public int getCoreNum()
    {
        return this.peProvisionerList.size();
    }

    public double getMips()
    {
        return this.peProvisionerList.get(0).getCapacity();
    }
}

