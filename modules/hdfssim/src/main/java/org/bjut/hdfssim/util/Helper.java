package org.bjut.hdfssim.util;

import com.opencsv.CSVWriter;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.Storage;
import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.util.*;

public class Helper {
    public static void printStorageUsage(Namenode namenode) {
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " SSD " + datanode
                        .getStorageByType(Storage.SSD).getUsedSize());
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " HDD " + datanode
                        .getStorageByType(Storage.HDD).getUsedSize());
            }
        }
    }

    public static void saveResult(List<Request> requestList, String path) {
        try {
            File file = new File(path);
            if(file.exists())
            {
                //file.renameTo(file.)
            }
            CSVWriter csvWriter = new CSVWriter(new FileWriter(file), ',');
            String[] head = {"fileSize", "submitTime", "finishedTime", "useTime"};
            csvWriter.writeNext(head);

            Iterator<Request> iterator = requestList.iterator();
            while (iterator.hasNext()) {
                Request r = iterator.next();
                String[] item = {Double.toString(r.gethFile().getSize()), Double.toString(r.getSubmitTime()),
                        Double.toString(r.getFinishedTime()), Double.toString(r.getFinishedTime() - r.getSubmitTime())};
                csvWriter.writeNext(item);
            }
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getConfigPath(String name)
    {
        return Configuration.getBasePath() + name + ".json";
    }
}
