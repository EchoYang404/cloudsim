package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.cloudbus.cloudsim.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

public class SelectDatanodeAllocationPolicy implements DatanodeAllocationPolicy {
    @Override
    public Datanode getDatanode(List<Block> blockList, Datanode addr) {
        int blockId = blockList.get(0).getId();
        Iterator<Block> iterator = blockList.get(0).gethFile().getReplicaListById(blockId).iterator();
        ArrayList<Double> list = new ArrayList<>();
        double sum = 0;
        while (iterator.hasNext()) {
            Block block = iterator.next();
            if (block.isMigrate()) {
                list.add(sum);
                continue;
            }

            HDFSHost host = block.getDatanode().getHost();
            double distance = addr.getDistance(block.getDatanode());
            //double tmp = 0.313 * (4 - distance) / 4 + 0.313 * host.getBwUtilization() + 0.5506 * host
            // .getDiskUtilization(block)/300 + 0.0935 * host
            // .getCpuUtilization();
            double tmp = 0.313 * ((4 - distance) / 4 + host.getBwUtilization() + host.getNetUtilization()) + 0.5506 *
                    host.getDiskUtilization(block) / 300 + 0.0935 * host.getCpuUtilization();
            sum += tmp;
            list.add(sum);
        }
        double select = new Random().nextDouble();
        for (int i = 0; i < list.size(); i++) {
            if ((list.get(i) / sum) > select && blockList.get(i).isMigrate() == false) {
                //Log.printLine( blockId + " "+ blockList.get(i).isMigrate() + " " + blockList.get(i).getDatanode().getId());
                return blockList.get(i).getDatanode();
            }
        }
        return null;
    }


//    @Override
//    public Datanode getDatanode(List<Block> blockList, Datanode addr) {
//        Iterator<Block> iterator = blockList.iterator();
//        double maxValue = Double.MIN_VALUE;
//        Datanode datanode = null;
//        while (iterator.hasNext()) {
//            Block block = iterator.next();
//            if (block.isMigrate()) {
//                continue;
//            }
//
//            HDFSHost host = block.getStorage().getDatanode().getHost();
//            double distance = addr.getDistance(block.getStorage().getDatanode());
//            double tmp = 0.313 * ((4 - distance) / 4 + host.getBwUtilization() + host.getNetUtilization()) + 0.5506
//                    * host.getDiskUtilization(block) / 300 + 0.0935 * host.getCpuUtilization();
//            if (tmp > maxValue) {
//                maxValue = tmp;
//                datanode = block.getStorage().getDatanode();
//            }
//        }
//        return datanode;
//    }
}
