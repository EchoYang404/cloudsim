package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;

import java.util.*;

public class Helper {
    public static void printStorageUsage(Namenode namenode) {
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " SSD " + datanode.getSSDStorageUsage());
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " HDD " + datanode.getHDDStorageUsage());
            }
        }
    }
}
