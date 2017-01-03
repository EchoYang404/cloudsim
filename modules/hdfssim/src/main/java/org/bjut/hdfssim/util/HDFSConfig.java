package org.bjut.hdfssim.util;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.bjut.hdfssim.models.HDFS.Namenode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class HDFSConfig {

    public List<DatanodeType> datanodeTypeList = new ArrayList<>();
    public List<DatanodeConfig> datanodeConfigList = new ArrayList<>();

    public HDFSConfig() {
    }

    public void createDatanodeList(Namenode namenode) {
        Iterator<DatanodeConfig> configIterator = datanodeConfigList.iterator();
        while (configIterator.hasNext()) {
            DatanodeConfig dc = configIterator.next();
            DatanodeType dt = datanodeTypeList.get(dc.typeNum);
            for (int i = 0; i < dc.count; i++) {
                namenode.addDatanode(new Datanode(dc.rackId, dt.hddCapacity, dt.hddMaxTransferRate, dt.ssdCapacity, dt.ssdMaxTransferRate, dt.bw, dt.cores, dt.mips));
            }
        }
    }
}
