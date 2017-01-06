package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.util.BlockConfig;
import org.bjut.hdfssim.util.HFileConfig;
import org.bjut.hdfssim.util.Id;

import java.io.Serializable;
import java.util.*;

public class HFile implements Serializable {
    private Integer id;
    private double size;
    private Map<Integer, List<Block>> blockList; // blockId, List<Block>
    private Namenode namenode;

    public HFile(HFileConfig config, Namenode namenode) {
        this.namenode = namenode;
        this.blockList = new HashMap<>();

        this.id = config.getId();
        this.size = config.getSize();

        Iterator<Map.Entry<Integer, List<BlockConfig>>> entryIterator = config.getBlockConfigList().entrySet()
                .iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<BlockConfig>> entry = entryIterator.next();
            Iterator<BlockConfig> iterator = entry.getValue().iterator();
            List<Block> blockList = new ArrayList<>();
            while (iterator.hasNext()) {
                BlockConfig blockConfig = iterator.next();
                blockList.add(new Block(this, entry.getKey(), blockConfig.getSize(), namenode
                        .getDatanodeByRackIdAndDatanodeId(blockConfig.getRackId(), blockConfig.getDatanodeId())
                        .getStorageByType(blockConfig.getStorageType())));
            }
            this.blockList.put(entry.getKey(),blockList);
        }
    }

    public HFile(double size, Namenode namenode) {
        this.id = Id.pollId(this.getClass());
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
            int blockId = Id.pollId(Block.class);
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
            tempList.add(new Block(this, blockId, blockSize));
        }
        return tempList;
    }

    public Map<Integer, List<Block>> getBlockList() {
        return blockList;
    }

    public List<Block> getReplicaListById(int blockId) {
        if (blockList.containsKey(blockId)) {
            return blockList.get(blockId);
        }
        return null;
    }

    public Integer getId() {
        return id;
    }

    public double getSize() {
        return size;
    }
}
