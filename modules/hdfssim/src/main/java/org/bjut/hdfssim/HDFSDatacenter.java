package org.bjut.hdfssim;

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.*;

public class HDFSDatacenter extends Datacenter{

    private List<HDFSHost> hostList;

    /**
     * Allocates a new Datacenter object.
     *
     * @param name               the name to be associated with this entity (as required by the super class)
     * @param characteristics    the characteristics of the datacenter to be created
     * @param vmAllocationPolicy the policy to be used to allocate VMs into hosts
     * @param storageList        a List of storage elements, for data simulation
     * @param schedulingInterval the scheduling delay to process each datacenter received event
     * @throws Exception when one of the following scenarios occur:
     *                   <ul>
     *                   <li>creating this entity before initializing CloudSim package
     *                   <li>this entity name is <tt>null</tt> or empty
     *                   <li>this entity has <tt>zero</tt> number of PEs (Processing Elements). <br/>
     *                   No PEs mean the Cloudlets can't be processed. A CloudResource must contain
     *                   one or more Machines. A Machine must contain one or more PEs.
     *                   </ul>
     * @pre name != null
     * @pre resource != null
     * @post $none
     */
    public HDFSDatacenter(String name, DatacenterCharacteristics characteristics, VmAllocationPolicy vmAllocationPolicy, List<org.cloudbus.cloudsim.Storage> storageList, double schedulingInterval) throws Exception {
        super(name, characteristics, vmAllocationPolicy, storageList, schedulingInterval);
        hostList = new ArrayList<>();
    }

    public boolean addHost(HDFSHost hdfsHost)
    {
        hostList.add(hdfsHost);
        hdfsHost.setDatacenter(this);
        return true;
    }
}
