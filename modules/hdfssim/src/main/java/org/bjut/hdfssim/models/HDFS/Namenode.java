package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDFSHost;
import org.bjut.hdfssim.HFile;
import org.bjut.hdfssim.util.Id;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.*;

public class Namenode implements Serializable {
    private double blockSize = 64;
    private int replicaCount = 3;
    private int rackCount = 10;
    private List<HFile> HFileList = new ArrayList<>();
    private Map<Integer, List<Datanode>> datanodeList = new HashMap<>(); // Map<rackId, List<datanode>>

    public double getBlockSize() {
        return blockSize;
    }

    public int getReplicaCount() {
        return replicaCount;
    }

    public void addDatanode(int rackId, Datanode datanode) {
        if (!datanodeList.containsKey(rackId)) {
            this.datanodeList.put(rackId, new ArrayList<>());
        }
        this.datanodeList.get(rackId).add(datanode);
    }


    public List<HFile> getHFileList() {
        return HFileList;
    }
    public Map<Integer, List<Datanode>> getDatanodeList() {
        return datanodeList;
    }

    public void setHFileList(List<HFile> HFileList) {
        if (datanodeList.isEmpty()) {
            try {
                throw new Exception("DatanodeList is empty!");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (HFile file : HFileList) {
            for (List<Block> replicaList : file.getBlockList().values()) {
                while(!addReplicaList(replicaList)){}
            }
        }
    }

    private boolean addReplicaList(List<Block> replicaList) {
        Random random = new Random();
        int rackId = random.nextInt(this.rackCount);
        int successNum = 0;
        int notAvailbleRack = rackId;

        Iterator<Block> blockIterator = replicaList.iterator();
        // add first block
        if (addBlockToRack(blockIterator.next(), rackId)) {
            successNum++;
        }
        // add second block
        if (addBlockToRack(blockIterator.next(), rackId)) {
            successNum++;
        }
        // add rest blocks

        while (blockIterator.hasNext()) {
            while(notAvailbleRack == (rackId = random.nextInt(this.rackCount))){}
            if (addBlockToRack(blockIterator.next(), rackId)) {
                successNum++;
            }
        }
        if (successNum != replicaList.size()) {
            resetReplicaStorage(replicaList);
            return false;
        }
        return true;
    }

    private boolean addBlockToRack(Block block, int rackId) {
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
}
