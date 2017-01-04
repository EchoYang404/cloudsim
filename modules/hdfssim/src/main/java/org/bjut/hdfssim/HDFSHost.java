package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.Request.ReadCloudlet;
import org.bjut.hdfssim.provisioners.ProvisionerForHDFS;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HDFSHost {
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

    private List<ProvisionerForHDFS> peProvisionerList;

    public HDFSHost(Datanode datanode, double ssdMaxTransferRate, double hddMaxTransferRate, double bw, int coreNum, double mips) {
        this.datanode = datanode;
        this.ssdProvisioner = new ProvisionerForHDFS(ssdMaxTransferRate, ReadCloudlet.DISK);
        this.hddProvisioner = new ProvisionerForHDFS(hddMaxTransferRate, ReadCloudlet.DISK);
        this.bwProvisioner = new ProvisionerForHDFS(bw, ReadCloudlet.BW);
        this.peProvisionerList = new ArrayList<>();
        for (int i = 0; i < coreNum; i++) {
            peProvisionerList.add(new ProvisionerForHDFS(mips, ReadCloudlet.CPU));
        }
    }

    public double excuteCloudlets(double time) {
        // excute all cpus
        Iterator<ProvisionerForHDFS> iterator = peProvisionerList.iterator();
        for (ProvisionerForHDFS provisioner : peProvisionerList) {
            provisioner.excuteCloudlets(time);
        }

        return 0;
    }

    public void addCloudLet(double time, ReadCloudlet cloudlet) {
        cloudlet.setHost(this);
        int minCPUNum = Integer.MAX_VALUE;
        ProvisionerForHDFS pe = null;
        for (ProvisionerForHDFS provisioner : peProvisionerList) {
            if (provisioner.getCurrentNum() < minCPUNum) {
                pe = provisioner;
                // TODO 为方便测试注释
                break;
                //minCPUNum = provisioner.getCurrentNum();
            }
        }
        List<ReadCloudlet> finishedCPUList = pe.getFinishedCloudLets(time);
        pe.addCloudlet(cloudlet, time);
        excuteFinishedCPUCloudlets(time, finishedCPUList);

    }

    private void excuteFinishedCPUCloudlets(double time, List<ReadCloudlet> finishedCPUList) {
        Iterator<ReadCloudlet> iterator = finishedCPUList.iterator();
        while (iterator.hasNext()) {
            ReadCloudlet cloudlet = iterator.next();
            double startTime = cloudlet.getCurrentStage().getFinishedTime();
            cloudlet.toNextStage();
            cloudlet.getCurrentStage().start(startTime);

            int type = cloudlet.getHost().datanode.getStorageTypeByBlockId(cloudlet.getBlockId());

            ProvisionerForHDFS provisioner = null;
            if (type == Storage.SSD) {
                provisioner = ssdProvisioner;
            } else {
                provisioner = hddProvisioner;
            }

            List<ReadCloudlet> finishedDiskList = provisioner.getFinishedCloudLets(time);
            provisioner.addCloudlet(cloudlet, time);
            excuteFinishedCPUCloudlets(time, finishedDiskList);
        }
    }

    private void excuteFinishedDiskCloudlets(double time, List<ReadCloudlet> finishedDiskList) {
        Iterator<ReadCloudlet> iterator = finishedDiskList.iterator();
        while (iterator.hasNext()) {
            ReadCloudlet cloudlet = iterator.next();
            double startTime = cloudlet.getCurrentStage().getFinishedTime();
            cloudlet.toNextStage();
            cloudlet.getCurrentStage().start(startTime);
            this.finishedList.addAll(bwProvisioner.getFinishedCloudLets(time));
            bwProvisioner.addCloudlet(cloudlet, time);
        }
    }
}
