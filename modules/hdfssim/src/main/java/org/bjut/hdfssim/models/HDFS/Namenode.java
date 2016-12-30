package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.HDFSHost;
import org.bjut.hdfssim.HFile;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Namenode {
    private static double blockSize = 64;
    private static int replicaCount = 3;
    private static List<HFile> HFileList = new ArrayList<>();
    private static Map<Integer, HDFSHost> datanodeList= new HashMap<>();

//    public Namenode(double blockSize, int replicaCount, int rackNum, )
//    {
//        this.blockSize = blockSize;
//        this.replicaCount = replicaCount;
//    }
}
