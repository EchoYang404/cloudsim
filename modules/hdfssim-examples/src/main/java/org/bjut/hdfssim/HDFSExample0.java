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
        // Second step : Create files
        List<HFile> fileList = createHFileByRadom(1000,1024,2048,namenode);
        // Third step : Add datanodes to namenode
        int rackNum = 10;
        int datanodeNum = 100;
        Random random = new Random();
        for (int i = 0; i < datanodeNum; i++) {
            int rackId = random.nextInt(10);
            namenode.addDatanode(rackId,new Datanode(rackId, 1048578,524288));
        }
        // Fourth step: Upload files to datanode
        namenode.setHFileList(fileList);

//        String path = Configuration.getBasePath() + "hdfs.txt";
//        try {
//            ObjectOutputStream oout = new ObjectOutputStream(new FileOutputStream(path));
//            oout.writeObject(namenode);
//            oout.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        printStorageUsage(namenode);
    }

    private static List<HFile> createHFileByRadom(int fileNum, int minSize, int maxSize, Namenode namenode)
    {
        List<HFile> fileList = new ArrayList<>();
        Random random = new Random();

        for(int i = 0; i < fileNum; i++)
        {
            double size = random.nextInt(maxSize - minSize) + minSize;
            HFile file = new HFile(size,namenode);
            fileList.add(file);
        }
        return fileList;
    }

    private static void printStorageUsage(Namenode namenode)
    {
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext())
        {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while(datanodeIterator.hasNext())
            {
                Datanode datanode = datanodeIterator.next();
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " SSD " +datanode.getSSDStorageUsage());
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " HDD " +datanode.getHDDStorageUsage());
            }
        }

    }
}
