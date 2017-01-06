package org.bjut.hdfssim.util;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HFile;

import java.util.*;

public class HFileConfig {
    private int id;
    private double size;
    private Map<Integer,List<BlockConfig>> blockList;

    public HFileConfig(HFile hFile)
    {
        this.id = hFile.getId();
        this.size = hFile.getSize();
        this.blockList = new HashMap<>();
        Iterator<Map.Entry<Integer,List<Block>>> entryIterator = hFile.getBlockList().entrySet().iterator();
        while (entryIterator.hasNext())
        {
            Map.Entry<Integer,List<Block>> entry = entryIterator.next();
            Iterator<Block> iterator = entry.getValue().iterator();
            List<BlockConfig> configList = new ArrayList<>();
            while (iterator.hasNext())
            {
                Block block = iterator.next();
                configList.add(new BlockConfig(block));
            }
            blockList.put(entry.getKey(), configList);
        }
    }

    public int getId() {
        return id;
    }

    public double getSize() {
        return size;
    }

    public Map<Integer, List<BlockConfig>> getBlockConfigList() {
        return blockList;
    }
}
