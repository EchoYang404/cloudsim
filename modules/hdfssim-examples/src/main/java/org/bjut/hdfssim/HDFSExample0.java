package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;

import javax.xml.crypto.Data;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.*;

public class HDFSExample0 {

    public static void main(String[] args) {
        // First step : Create namenode
        Namenode namenode = new Namenode();
        // Second step : Create datanodes from Config Files
        namenode.createDatanodeFromConfig(Configuration.getBasePath() + "HDFSConfig.json");
        // Third step : Create files and upload files
        namenode.createHFileByRadom(1000,1024,2048);

        Map<Integer,HDFSHost> hostList = namenode.getHDFSHostList(); // datanodeId, HDFSHost

        Helper.printStorageUsage(namenode);
    }
}
