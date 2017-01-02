package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.util.Id;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HFile {
    private Integer id = Id.pollId(this.getClass());
    private double size;
    private Map<Integer, List<Block>> blockList; // blockId, List<Block>
    private Namenode namenode;

    public HFile(double size, Namenode namenode) {
        this.size = size;
        blockList = new HashMap<>();
        this.namenode = namenode;
        creatBlocks();
    }

    private void creatBlocks() {
        double blockSize = this.namenode.getBlockSize();
        int blockNum = (int) Math.floor(size / blockSize);
        double restSize = size;
        int repilcaCount = this.namenode.getReplicaCount();
        for (int i = 0; i < blockNum; i++) {
            List<Block> tempList = null;
            int blockId = Id.pollId(Block.class.getClass());
            if (restSize > blockSize) {
                tempList = creatBlock(blockId, blockSize, repilcaCount);
            } else {
                tempList = creatBlock(blockId, restSize, repilcaCount);
            }
            this.blockList.put(blockId, tempList);
            restSize -= blockSize;
        }
    }

    private List<Block> creatBlock(int blockId, double blockSize, int repilcaCount) {
        List<Block> tempList = new ArrayList<>();
        for (int j = 0; j < repilcaCount; j++) {
            tempList.add(new Block(this,blockId, blockSize));
        }
        return tempList;
    }

    public Map<Integer, List<Block>> getBlockList() {
        return blockList;
    }

    public List<Block> getReplicaListById(int blockId)
    {
        if(blockList.containsKey(blockId))
        {
            return blockList.get(blockId);
        }
        return null;
    }
}
