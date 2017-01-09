package org.bjut.hdfssim.models.HDFS;

import com.google.gson.Gson;
import org.bjut.hdfssim.*;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.config.HDFSConfig;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public class Namenode implements Serializable {
    private double blockSize;
    private int replicaCount;
    private List<HFile> HFileList;
    private Map<Integer, List<Datanode>> datanodeList; // Map<rackId, List<datanode>>

    public Namenode() {
        this.blockSize = Configuration.getIntProperty("blockSize");
        this.replicaCount = Configuration.getIntProperty("replicaCount");;
        init();
    }

    private void init()
    {
        this.HFileList = new ArrayList<>();
        this.datanodeList = new HashMap<>();
    }

    public double getBlockSize() {
        return blockSize;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    public void addDatanode(Datanode datanode) {
        if (!datanodeList.containsKey(datanode.getRackId())) {
            this.datanodeList.put(datanode.getRackId(), new ArrayList<>());
        }
        this.datanodeList.get(datanode.getRackId()).add(datanode);
    }


    public List<HFile> getHFileList() {
        return HFileList;
    }

    public Map<Integer, List<Datanode>> getDatanodeList() {
        return datanodeList;
    }

    private void uploadFiles() {
        if (this.datanodeList.isEmpty()) {
            try {
                throw new Exception("DatanodeList is empty!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        if (this.HFileList.isEmpty()) {
            try {
                throw new Exception("FileList is empty!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (HFile file : HFileList) {
            for (List<Block> replicaList : file.getBlockList().values()) {
                while (!uploadReplica(replicaList)) {
                }
            }
        }
    }

    private boolean uploadReplica(List<Block> replicaList) {
        Random random = new Random();
        int rackCount = this.datanodeList.size();
        int rackId = random.nextInt(rackCount);
        int successNum = 0;
        int notAvailbleRack = rackId;
        int ssdNum = 1;
        int type;

        Iterator<Block> blockIterator = replicaList.iterator();
        // add first block
        type = uploadBlockToRack(blockIterator.next(), rackId, ssdNum);
        if (type == Storage.HDD || type == Storage.SSD) {
            successNum++;
            if (type == Storage.SSD) {
                ssdNum--;
            }
        }

        // add second block
        type = uploadBlockToRack(blockIterator.next(), rackId, ssdNum);
        if (type == Storage.HDD || type == Storage.SSD) {
            successNum++;
            if (type == Storage.SSD) {
                ssdNum--;
            }
        }

        // add rest blocks
        while (blockIterator.hasNext()) {
            while (notAvailbleRack == (rackId = random.nextInt(rackCount))) {
            }
            type = uploadBlockToRack(blockIterator.next(), rackId, ssdNum);
            if (type == Storage.HDD || type == Storage.SSD) {
                successNum++;
                if (type == Storage.SSD) {
                    ssdNum--;
                }
            }
        }
        if (successNum != replicaList.size()) {
            resetReplicaStorage(replicaList);
            return false;
        }
        return true;
    }

    private int uploadBlockToRack(Block block, int rackId, int ssdNum) {
        int type = -1;
        for (int i = 0; i < 10; i++) {
            Datanode node = this.getRandomDatanodeIdByRack(rackId);
            if (ssdNum > 0) {
                type = node.addBlockToStorage(block, Storage.SSD);
            } else {
                type = node.addBlockToStorage(block, Storage.HDD);
            }
            if (type == Storage.HDD || type == Storage.SSD) {
                return type;
            }
        }
        // 创建失败返回-1
        return type;
    }

    private void resetReplicaStorage(List<Block> replicaList) {
        for (Block block : replicaList) {
            if (block.getStorage() != null) {
                block.getStorage().deleteBlock(block);
            }
        }
    }


    public Datanode getRandomDatanodeIdByRack(int rackId) {
        if (this.datanodeList.containsKey(rackId)) {
            List<Datanode> rackDatanodeList = this.datanodeList.get(rackId);
            Random random = new Random();
            int num = random.nextInt(rackDatanodeList.size());
            return rackDatanodeList.get(num);
        } else {
            try {
                throw new Exception("rackId " + rackId + "has no Datanode!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public Map<Integer, HDFSHost> getHDFSHostList() {
        if (this.datanodeList.isEmpty()) {
            try {
                throw new Exception("HDFSHostList is empty!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Map<Integer, HDFSHost> HDFSHostList = new HashMap<>();
        Iterator<Map.Entry<Integer, List<Datanode>>> entries = this.datanodeList.entrySet().iterator();
        while (entries.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entries.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                HDFSHostList.put(datanode.getId(), datanode.getHost());
            }
        }
        return HDFSHostList;
    }

    public void createDatanodeFromConfigFile(String path) {
        Gson gson = new Gson();
        try {
            FileReader fr = new FileReader(path);
            HDFSConfig config = gson.fromJson(fr, HDFSConfig.class);
            config.createDatanodeList(this);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void createHFileByRadom(int fileNum, int minSize, int maxSize) {
        Random random = new Random();

        for (int i = 0; i < fileNum; i++) {
            double size = random.nextInt(maxSize - minSize) + minSize;
            this.HFileList.add(new HFile(size, this));
        }
        uploadFiles();
    }

    public List<Request> getRquestListFromConfigFile(String path)
    {
        Gson gson = new Gson();
        try {
            FileReader fr = new FileReader(path);
            HDFSConfig config = gson.fromJson(fr, HDFSConfig.class);
            return config.getRequestList(this);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public void resetStorages() {
        Iterator<List<Datanode>> listIterator = this.datanodeList.values().iterator();
        while (listIterator.hasNext()) {
            Iterator<Datanode> iterator = listIterator.next().iterator();
            while (iterator.hasNext()) {
                Datanode datanode = iterator.next();
                datanode.resetStorages();
            }
        }
    }

    public void addHFile(HFile hFile) {
        this.HFileList.add(hFile);
    }

    public Datanode getDatanodeByRackIdAndDatanodeId(int rackId, int datanodeId) {
        Iterator<Datanode> iterator = this.datanodeList.get(rackId).iterator();
        while (iterator.hasNext()) {
            Datanode datanode = iterator.next();
            if (datanode.getId() == datanodeId) {
                return datanode;
            }
        }
        try {
            throw new Exception("There is no datanode has id = " + datanodeId + "!!");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public HFile getHFileById(int hFileId)
    {
        Iterator<HFile> iterator = this.HFileList.iterator();
        while (iterator.hasNext())
        {
            HFile hFile = iterator.next();
            if(hFile.getId() == hFileId)
            {
                return hFile;
            }
        }
        return null;
    }

    public void initFromConfigFile(String path)
    {
        init();
        Gson gson = new Gson();
        try {
            FileReader fr = new FileReader(path);
            HDFSConfig config = gson.fromJson(fr, HDFSConfig.class);
            config.setDatanodeList(this);
            config.setHFileList(this);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
