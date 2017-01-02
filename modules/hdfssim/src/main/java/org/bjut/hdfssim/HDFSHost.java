package org.bjut.hdfssim;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.provisioners.BwProvisioner;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisioner;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import java.util.ArrayList;
import java.util.List;

public class HDFSHost extends Host {

    private HDDStorage hddStorage;
    private SSDStorage ssdStorage;
    private HDFSDatacenter datacenter;
    /**
     * Instantiates a new host.
     *
     * @param id             the host id
     * @param ramProvisioner the ram provisioner
     * @param bwProvisioner  the bw provisioner
     * @param storage        the storage capacity
     * @param peList         the host's PEs list
     * @param vmScheduler    the vm scheduler
     */

    public HDFSHost(int id, double hddCapacity, double ssdCapacity, RamProvisioner ramProvisioner, BwProvisioner bwProvisioner, long storage, List<? extends Pe> peList, VmScheduler vmScheduler) {
        super(id, ramProvisioner, bwProvisioner, storage, peList, vmScheduler);
        hddStorage = new HDDStorage(hddCapacity);
        ssdStorage = new SSDStorage(ssdCapacity);

    }
    public HDFSHost(int id, double hddCapacity, double ssdCapacity)
    {
        super(id, new RamProvisionerSimple(2048), new BwProvisionerSimple(10000), 0, new ArrayList<Pe>(), new VmSchedulerTimeShared(new ArrayList<Pe>()));
        hddStorage = new HDDStorage(hddCapacity);
        ssdStorage = new SSDStorage(ssdCapacity);
    }

//    public boolean addBlocktoHDDStorage(Block block)
//    {
//        return hddStorage.addBlock(block);
//    }

//    public boolean addBlocktoSSDStorage(Block block)
//    {
//        return ssdStorage.addBlock(block);
//    }

    public boolean setDatacenter(HDFSDatacenter datacenter)
    {
        this.datacenter = datacenter;
        return true;
    }

    public Datacenter getDatacenter()
    {
        return this.datacenter;
    }
}
