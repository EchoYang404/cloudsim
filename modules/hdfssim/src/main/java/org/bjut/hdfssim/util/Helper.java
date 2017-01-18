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

    public static void printStorageAccessTime(Namenode namenode) {
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();

                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " SSD acc " + datanode
                        .getInfo().getSsdAccessCount());
                System.out.println("RId " + entry.getKey() + " DId " + datanode.getId() + " HDD acc " + datanode
                        .getInfo().getHddAccessCount());
            }
        }
    }

    public static void saveResult(List<Request> requestList, String path) {
        try {
            CSVWriter csvWriter = new CSVWriter(new FileWriter(new File(path)), ',');
            String[] head = {"fileSize", "submitTime", "finishedTime", "useTime", "=sum(D2:D" + (requestList.size() +
                    1) + ")/" + requestList.size()};
            csvWriter.writeNext(head);

            Iterator<Request> iterator = requestList.iterator();
            while (iterator.hasNext()) {
                Request r = iterator.next();
                String[] item = {Double.toString(r.gethFile().getSize()), Double.toString(r.getSubmitTime()),
                        Double.toString(r.getFinishedTime()), Double.toString(r.getFinishedTime() - r.getSubmitTime()
                ), Integer.toString(r.gethFile().getBlockList().size()), Integer.toString(r.getCurrentNum())};
                csvWriter.writeNext(item);
            }
            csvWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void saveExResult(Namenode namenode, List<Request> requestList, int exType) {
        String path = Configuration.getBasePath() + Configuration.getStringProperty("resultPath") + namenode
                .getBlockSize() + "_" + namenode.getReplicaCount() + ".csv";
        File file = new File(path);
        if (!file.getParentFile().exists()) {
            //如果目标文件所在的目录不存在，则创建父目录
            System.out.println("目标文件所在目录不存在，准备创建它！");
            if (!file.getParentFile().mkdirs()) {
                try {
                    throw new Exception("创建目标文件所在目录失败！");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (!file.exists()) {
            try {
                FileWriter writer = new FileWriter(path, true);
                writer.write("requestCount,finishedCount,finishedTime,totalSize,totalUseTime,avgSpeed,Type," +
                        "SSD_acc_1,SSD_use_1,HDD_acc_1,HDD_use_1\n");
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileWriter writer = new FileWriter(path, true);
            StringBuffer sb = new StringBuffer();
            sb.append(requestList.size() + ",");
            double totalUseTime = 0;
            double totalSize = 0;
            double finishedTime = 0;
            int num = 0;
            for (Request r : requestList) {
                if (r.isFinished()) {
                    totalSize += r.gethFile().getSize();
                    totalUseTime += r.getFinishedTime() - r.getSubmitTime();
                    if (r.getFinishedTime() > finishedTime) {
                        finishedTime = r.getFinishedTime();
                    }
                    num++;
                }
            }
            sb.append(num + ",");
            sb.append(finishedTime + ",");
            sb.append(Double.toString(totalSize) + ",");
            sb.append(Double.toString(totalUseTime) + ",");
            sb.append(Double.toString(totalSize / totalUseTime) + ",");
            sb.append(exType + ",");
            Iterator<List<Double>> listIterator = namenode.getRackAccessTime().values().iterator();
            while (listIterator.hasNext()) {
                List<Double> list = listIterator.next();
                sb.append(list.get(0) + ",");
                sb.append(list.get(1) + ",");
                sb.append(list.get(2) + ",");
                sb.append(list.get(3) + ",");
            }
            String result = sb.substring(0, sb.length() - 1);
            writer.write(result + '\n');
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getConfigPath(String name) {
        return Configuration.getBasePath() + name + ".json";
    }
}
