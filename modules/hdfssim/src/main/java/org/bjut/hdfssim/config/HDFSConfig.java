package org.bjut.hdfssim.config;

import com.google.gson.Gson;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HFile;
import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.util.Helper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;


public class HDFSConfig {
    private double blockSize;
    private int replicaCount;

    private List<DatanodeType> datanodeTypeList;
    private List<RackConfig> rackConfigList;

    private List<DatanodeConfig> datanodeConfigList;
    private List<HFileConfig> fileConfigList;

    private List<RequestConfig> requestConfigList;

    public HDFSConfig() {
        datanodeTypeList = new ArrayList<>();
        rackConfigList = new ArrayList<>();

        datanodeConfigList = new ArrayList<>();
        fileConfigList = new ArrayList<>();

        requestConfigList = new ArrayList<>();
    }

    public void setBlockSize(double blockSize) {
        this.blockSize = blockSize;
    }

    public void setReplicaCount(int replicaCount) {
        this.replicaCount = replicaCount;
    }

    public double getBlockSize() {
        return blockSize;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    // 根据 DatanodeType与RackConfig 创建Datanode并添加至namenode
    public void createDatanodeList(Namenode namenode) {
        Iterator<RackConfig> configIterator = rackConfigList.iterator();
        while (configIterator.hasNext()) {
            RackConfig rc = configIterator.next();
            DatanodeType dt = datanodeTypeList.get(rc.getTypeNum());
            for (int i = 0; i < rc.getCount(); i++) {
                namenode.addDatanode(new Datanode(rc.getRackId(), dt.hddCapacity, dt.hddMaxTransferRate, dt.ssdCapacity,
                        dt.ssdMaxTransferRate, dt.bw, dt.cores, dt.mips));
            }
        }
    }

    // 根据HFileConfig 创建Hfile并添加至namenode
    public void setHFileList(Namenode namenode) {
        Iterator<HFileConfig> configIterator = fileConfigList.iterator();
        while (configIterator.hasNext()) {
            HFileConfig hc = configIterator.next();
            namenode.addHFile(new HFile(hc, namenode));
        }
    }

    // 根据DatanodeConfig 创建Datanode并添加至namenode
    public void setDatanodeList(Namenode namenode) {
        Iterator<DatanodeConfig> iterator = datanodeConfigList.iterator();
        while (iterator.hasNext()) {
            DatanodeConfig dc = iterator.next();
            namenode.addDatanode(new Datanode(dc));
        }
    }

    // 根据namenode中已有的datanode，创建DatanodeConfig
    public void setDatanodeConfigList(Namenode namenode) {
        Iterator<List<Datanode>> listIterator = namenode.getDatanodeList().values().iterator();
        while (listIterator.hasNext()) {
            Iterator<Datanode> iterator = listIterator.next().iterator();
            while (iterator.hasNext()) {
                Datanode datanode = iterator.next();
                this.datanodeConfigList.add(new DatanodeConfig(datanode));
            }
        }
        this.blockSize = namenode.getBlockSize();
        this.replicaCount = namenode.getReplicaCount();
    }

    // 根据namenode中已有的HFile，创建HFileConfig
    public void setHFileConfigList(Namenode namenode) {
        Iterator<HFile> iterator = namenode.getHFileList().iterator();
        while (iterator.hasNext()) {
            HFile hFile = iterator.next();
            this.fileConfigList.add(new HFileConfig(hFile));
        }
    }

    // 根据namenode中已有的Datanode与HFile，创建RequestConfig
//    public void setRequestConfigList(Namenode namenode, int requestCount) {
//        //List<Request> requestList = new ArrayList<>();
//        int rackCount = namenode.getDatanodeList().size();
//        // TODO 访问的文件范围
//        int fileCount = 150;
//        //int fileCount = (int) Math.ceil(namenode.getHFileList().size() * 0.2);
//        // TODO 读取请求均匀到达
//        double submitTime = 0;
//        double interval = Configuration.getDoubleProperty("totalTime")/requestCount;
//        Random random = new Random();
//        for (int i = 0; i < requestCount; i++) {
//            int rackId = random.nextInt(rackCount);
//            this.requestConfigList.add(new RequestConfig(submitTime, rackId, namenode.getRandomDatanodeIdByRack
//                    (rackId).getId(), 1 + random.nextInt(fileCount)));
//            submitTime += interval;
//        }
//    }

    public void setRequestConfigList(Namenode namenode, int requestCount) {
        int rackCount = namenode.getDatanodeList().size();
        // TODO 访问的文件范围
        int fileCount = 150;
        //int fileCount = (int) Math.ceil(namenode.getHFileList().size() * 0.2);
        double submitTime = 0;
        double interval = Configuration.getDoubleProperty("ArriveInterval");
        double lamda = Configuration.getDoubleProperty("lamda");
        int restCount = requestCount;
        int x = 0;
        while (restCount > 0){
            int num = Helper.getPosionNum(lamda,x,requestCount);
            x++;
            if(num > restCount){
                num = restCount;
                restCount = 0;
            }else {
                restCount -= num;
            }
            Random random = new Random();
            for (int i = 0; i < num; i++) {
                int rackId = random.nextInt(rackCount);
                this.requestConfigList.add(new RequestConfig(submitTime, rackId, namenode.getRandomDatanodeIdByRack
                        (rackId).getId(), 1 + random.nextInt(fileCount)));
            }
            submitTime += interval;
        }
    }

    public List<Request> getRequestList(Namenode namenode) {
        List<Request> requestList = new ArrayList<>();
        Iterator<RequestConfig> iterator = this.requestConfigList.iterator();
        while (iterator.hasNext()) {
            RequestConfig rc = iterator.next();

            Datanode datanode = namenode.getDatanodeByRackIdAndDatanodeId(rc.getRackId(), rc.getDatanodeId());
            HFile hFile = namenode.getHFileById(rc.getHfileId());
            requestList.add(new Request(datanode, hFile, rc.getSubmitTime()));
        }
        return requestList;
    }

    // 将所有配置信息写入至json文件中
    public void writeToFile(String path) {
        Gson gson = new Gson();
        try {
            File file = new File(path);
            if(!file.getParentFile().exists()) {
                //如果目标文件所在的目录不存在，则创建父目录
                System.out.println("目标文件所在目录不存在，准备创建它！");
                if(!file.getParentFile().mkdirs()) {
                    throw new Exception("创建目标文件所在目录失败！");
                }
            }
            FileWriter fw = new FileWriter(file);
            String res = gson.toJson(this);
            fw.write(gson.toJson(this));
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<DatanodeType> getDatanodeTypeList() {
        return datanodeTypeList;
    }

    public List<RackConfig> getRackConfigList() {
        return rackConfigList;
    }
}
