package org.bjut.hdfssim;

import java.util.ArrayList;
import java.util.List;

public class HFile {
    private Integer id;
    private double size;
    private List<Block> blockList;

    public HFile(Integer id, double size)
    {
        this.id = id;
        this.size = size;
        blockList = new ArrayList<>();
    }

    private void createBlocks(double size)
    {
        int blockSize = Configuration.getIntProperty("blockSize");
        int blockNum = (int) Math.floor(size / blockSize);
        double restSize = size;
        for(int i = 0; i < blockNum; i++)
        {
            blockList.add(new Block(blockSize));
            restSize -= blockSize;
        }
        if(restSize > 0)
        {
            blockList.add(new Block(restSize));
        }
    }
}
