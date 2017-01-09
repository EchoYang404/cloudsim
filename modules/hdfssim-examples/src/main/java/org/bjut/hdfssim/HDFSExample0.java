package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.config.CreateConfig;

import java.util.*;

public class HDFSExample0 {

    public static void main(String[] args) {
        String path = Configuration.getBasePath() + "HDFSConfig.json";
        CreateConfig.excute(path);
        // First step : Create namenode
        //Namenode namenode = new Namenode();
        // Second step : Create datanodes from Config Files
        //namenode.setDatanodesFromConfigFile(path);
        // Third step : Create HFiles from Config Files and upload to namenode
        //namenode.setHFilesFromConfigFile(path);

        //Map<Integer,HDFSHost> hostList = namenode.getHDFSHostList(); // datanodeId, HDFSHost

        //Helper.printStorageUsage(namenode);
    }
}
