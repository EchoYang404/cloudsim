package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.provisioners.BwProvisionerForHDFS;
import org.bjut.hdfssim.provisioners.PeProvisionerForHDFS;
import org.bjut.hdfssim.provisioners.StorageProvisionerForHDFS;
import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.util.ArrayList;
import java.util.List;

public class HDFSHost {
    /**
     * The id of the HDFSHost.
     */
    private int id;

    private Datanode datanode;

    /**
     * The ssd provisioner for hdfs.
     */
    private StorageProvisionerForHDFS ssdProvisioner;

    /**
     * The hdd provisioner for hdfs.
     */
    private StorageProvisionerForHDFS hddProvisioner;

    /**
     * The bw provisioner for hdfs.
     */
    private BwProvisionerForHDFS bwProvisioner;

    private List<PeProvisionerForHDFS> peProvisionerList;

    public HDFSHost(Datanode datanode, double ssdMaxTransferRate, double hddMaxTransferRate, double bw, int coreNum, double mips) {
        this.datanode = datanode;
        this.ssdProvisioner = new StorageProvisionerForHDFS(ssdMaxTransferRate);
        this.hddProvisioner = new StorageProvisionerForHDFS(hddMaxTransferRate);
        this.bwProvisioner = new BwProvisionerForHDFS(bw);
        this.peProvisionerList = new ArrayList<>();
        for(int i = 0; i < coreNum; i++)
        {
            peProvisionerList.add(new PeProvisionerForHDFS(mips));
        }
    }
}
