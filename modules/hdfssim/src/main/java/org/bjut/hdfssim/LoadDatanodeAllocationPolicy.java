package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;

import java.util.Iterator;
import java.util.List;

public class LoadDatanodeAllocationPolicy implements DatanodeAllocationPolicy {
    @Override
    public Datanode getDatanode(List<Block> blockList, Datanode addr) {
        Iterator<Block> iterator = blockList.iterator();
        double load = Double.MIN_VALUE;
        Datanode datanode = null;
        while (iterator.hasNext()) {
            Block block = iterator.next();
            HDFSHost host = block.getStorage().getDatanode().getHost();
            double distance = addr.getDistance(block.getStorage().getDatanode());
            //double tmp = 0.313 * (4 - distance) / 4 + 0.313 * host.getBwUtilization() + 0.5506 * host.getDiskUtilization(block)/300 + 0.0935 * host
            // .getCpuUtilization();
            double tmp = 0.313 * (4 - distance) / 4 + 0.5506 * host.getDiskUtilization
                    (block) / 300 + 0.0935 * host.getCpuUtilization();
            if (tmp > load) {
                datanode = host.getDatanode();
                load = tmp;
            }
        }
        return datanode;
    }
}
