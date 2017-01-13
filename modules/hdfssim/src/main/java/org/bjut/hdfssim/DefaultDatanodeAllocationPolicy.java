package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class DefaultDatanodeAllocationPolicy implements DatanodeAllocationPolicy {
    @Override
    public Datanode getDatanode(List<Block> blockList, Datanode addr) {
        Iterator<Block> iterator = blockList.iterator();
        Datanode datanode = null;
        int minDis = Integer.MAX_VALUE;
        int distance;
        while (iterator.hasNext()) {
            Block block = iterator.next();
            if (block.isMigrate()) {
                continue;
            }
            distance = addr.getDistance(block.getDatanode());
            if (distance == minDis && new Random().nextDouble() <= 0.8) {
                datanode = block.getStorage().getDatanode();
            } else if (distance < minDis) {
                datanode = block.getDatanode();
                minDis = addr.getDistance(block.getDatanode());
            }
        }
        return datanode;
    }
}
