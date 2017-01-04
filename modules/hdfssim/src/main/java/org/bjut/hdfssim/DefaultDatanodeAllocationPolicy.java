package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.List;

public class DefaultDatanodeAllocationPolicy implements DatanodeAllocationPolicy{
    @Override
    public Datanode getDatanode(List<Block> blockList, Datanode addr) {
        Iterator<Block> iterator = blockList.iterator();
        Datanode datanode = null;
        int distance = Integer.MAX_VALUE;
        while (iterator.hasNext())
        {
            Block block = iterator.next();
            if(addr.getDistance(block.getStorage().getDatanode()) < distance)
            {
                datanode = block.getStorage().getDatanode();
            }
        }
        return datanode;
    }
}
