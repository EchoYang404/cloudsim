package org.bjut.hdfssim.config;

import org.bjut.hdfssim.Block;

public class BlockConfig {
    private int rackId;
    private int datanodeId;
    private double size;
    private int storageType;

    public BlockConfig(Block block)
    {
        this.rackId = block.getStorage().getDatanode().getRackId();
        this.datanodeId = block.getStorage().getDatanode().getId();
        this.size = block.getSize();
        this.storageType = block.getStorage().getDatanode().getStorageTypeByBlockId(block.getId());
    }

    public int getRackId() {
        return rackId;
    }

    public int getDatanodeId() {
        return datanodeId;
    }

    public double getSize() {
        return size;
    }

    public int getStorageType() {
        return storageType;
    }
}
