package org.bjut.hdfssim.models.HDFS;

import com.google.gson.Gson;
import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDFSHost;
import org.bjut.hdfssim.HFile;
import org.bjut.hdfssim.util.HDFSConfig;

import javax.xml.crypto.Data;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;

public class Namenode implements Serializable {
    private double blockSize = 64;
    private int replicaCount = 3;
    private List<HFile> HFileList = new ArrayList<>();
    private Map<Integer, List<Datanode>> datanodeList = new HashMap<>(); // Map<rackId, List<datanode>>

    public Namenode() {}

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
                while(!uploadReplica(replicaList)){}
            }
        }
    }

    private boolean uploadReplica(List<Block> replicaList) {
        Random random = new Random();
        int rackCount = this.datanodeList.size();
        int rackId = random.nextInt(rackCount);
        int successNum = 0;
        int notAvailbleRack = rackId;

        Iterator<Block> blockIterator = replicaList.iterator();
        // add first block
        if (uploadBlockToRack(blockIterator.next(), rackId)) {
            successNum++;
        }
        // add second block
        if (uploadBlockToRack(blockIterator.next(), rackId)) {
            successNum++;
        }
        // add rest blocks

        while (blockIterator.hasNext()) {
            while(notAvailbleRack == (rackId = random.nextInt(rackCount))){}
            if (uploadBlockToRack(blockIterator.next(), rackId)) {
                successNum++;
            }
        }
        if (successNum != replicaList.size()) {
            resetReplicaStorage(replicaList);
            return false;
        }
        return true;
    }

    private boolean uploadBlockToRack(Block block, int rackId) {
        Datanode node = this.getRandomDatanodeIdByRack(rackId);
        int num = 10;
        while (!node.addBlock(block) && num != 0) {
            node = this.getRandomDatanodeIdByRack(rackId);
            num--;
        }
        if (num == 0) {
            return false;
        }
        return true;
    }

    private void resetReplicaStorage(List<Block> replicaList) {
        for (Block block : replicaList) {
            block.setStorage(null);
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

    public Map<Integer, HDFSHost> getHDFSHostList()
    {
        if (this.datanodeList.isEmpty()) {
            try {
                throw new Exception("HDFSHostList is empty!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Map<Integer, HDFSHost> HDFSHostList = new HashMap<>();
        Iterator<Map.Entry<Integer, List<Datanode>>> entries = this.datanodeList.entrySet().iterator();
        while(entries.hasNext())
        {
            Map.Entry<Integer, List<Datanode>> entry = entries.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while(datanodeIterator.hasNext())
            {
                Datanode datanode = datanodeIterator.next();
                HDFSHostList.put(datanode.getId(),datanode.getHost());
            }
        }
        return HDFSHostList;
    }

    public void createDatanodeFromConfig(String path)
    {
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
}
