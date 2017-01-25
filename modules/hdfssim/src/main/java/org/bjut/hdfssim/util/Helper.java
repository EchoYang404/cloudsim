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
//                writer.write("requestCount,finishedCount,finishedTime,totalSize,totalUseTime,avgSpeed,Type," +
//                        "SSD_acc_1,SSD_use_1,HDD_acc_1,HDD_use_1\n");
                writer.write("requestCount,finishedCount,finishedTime,totalSize,totalUseTime,avgSpeed,Type," +
                        "SSDAcc,SSDUsage,inDatanode,inRack,betweenRack\n");
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
            double acc = 0;
            double usage = 0;
            while (listIterator.hasNext()) {
                List<Double> list = listIterator.next();
                acc += list.get(0);
                usage += list.get(1);
//                sb.append(list.get(0) + ",");
//                sb.append(list.get(1) + ",");
//                sb.append(list.get(2) + ",");
//                sb.append(list.get(3) + ",");
            }
            sb.append(acc + ",");
            sb.append(usage + ",");
            sb.append(namenode.getMigrationer().getSizeInDatanode() + ",");
            sb.append(namenode.getMigrationer().getSizeInRack() + ",");
            sb.append(namenode.getMigrationer().getSizeBetweenRack() + ",");
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

    public static int getPosionNum(double lamda, int x, int totalNum) {
        return (int) Math.ceil(Math.pow(lamda, x) * Math.exp(-lamda) / fact(x) * totalNum);
    }

    private static int fact(int n) {
        if (n <= 1) {
            return 1;
        } else {
            return n * fact(n - 1);
        }
    }
}
