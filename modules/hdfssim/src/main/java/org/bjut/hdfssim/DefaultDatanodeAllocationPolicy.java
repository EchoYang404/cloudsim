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
            distance = addr.getDistance(block.getStorage().getDatanode());
            if (distance == minDis && new Random().nextInt(100) < 50) {
                datanode = block.getStorage().getDatanode();
            } else if (distance < minDis) {
                datanode = block.getStorage().getDatanode();
                minDis = addr.getDistance(block.getStorage().getDatanode());
            }
        }
        return datanode;
    }
}
