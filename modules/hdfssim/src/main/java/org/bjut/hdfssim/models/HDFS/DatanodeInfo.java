package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.Storage;

import java.util.*;

public class DatanodeInfo {
    private Datanode datanode;
    private Map<Block, List<Double>> ssdHistory;
    private Map<Block, List<Double>> hddHistory;

    private int ssdAccessCount;
    private int hddAccessCount;

    private boolean hasNewSsdAccess;
    private boolean hasNewHddAccess;

    public DatanodeInfo(Datanode datanode) {
        this.datanode = datanode;
        this.ssdHistory = new HashMap<>();
        this.hddHistory = new HashMap<>();
        this.ssdAccessCount = 0;
        this.hddAccessCount = 0;
        this.hasNewHddAccess = false;
        this.hasNewSsdAccess = false;
    }

    public void addSsdBlock(Block block) {
        this.ssdHistory.put(block, new ArrayList<>());
    }

    public void addHddBlock(Block block) {
        this.hddHistory.put(block, new ArrayList<>());
    }

    public List<Double> deleteSsdBlock(Block block) {
        List<Double> value = this.ssdHistory.get(block);
        this.ssdHistory.remove(block);
        return value;
    }

    public List<Double> deleteHddBlock(Block block) {
        List<Double> value = this.hddHistory.get(block);
        this.hddHistory.remove(block);
        return value;
    }

    public void accessBlock(Block block, double time) {
        block.access();

        Map<Block, List<Double>> m;
        if (block.getStorage().getType() == Storage.SSD) {
            m = ssdHistory;
            ssdAccessCount++;
            hasNewSsdAccess = true;
        } else {
            m = hddHistory;
            hddAccessCount++;
            hasNewHddAccess = true;
        }
        m.get(block).add(time);
    }

    public void finishAccessBlock(Block block) {
        block.finishAccess();
    }


    public void updateBlockHistory(Block block, List<Double> accessHistory) {
        if (ssdHistory.containsKey(block)) {
            ssdHistory.put(block, accessHistory);
        } else {
            hddHistory.put(block, accessHistory);
        }
    }

    public int getHddAccessCount() {
        return hddAccessCount;
    }

    public int getSsdAccessCount() {
        return ssdAccessCount;
    }

    public List<Block> getGreaterFromHdd(double frequecy, double time) {
        List<Block> result = new ArrayList<>();
        if(!hasNewHddAccess){
            return result;
        }
        hasNewHddAccess = false;
        Iterator<Block> iterator = hddHistory.keySet().iterator();
        while (iterator.hasNext()) {
            Block block = iterator.next();
            if (hddHistory.get(block).size() == 0 || block.isMigrate()) {
                continue;
            }
            updateBlockHistory(hddHistory.get(block).iterator(),time);

            double f = (double) hddHistory.get(block).size() / Configuration.getIntProperty("maxFrequencyInterval");
            if (f >= frequecy) {
                if (!block.isMigrate()) {
                    block.setMigrate(true);
                    result.add(block);
                }
            }
        }
        return result;
    }

    public List<Block> getLessFromSsd(double frequecy, double time) {
        List<Block> result = new ArrayList<>();
        if(!hasNewSsdAccess){
            return result;
        }
        hasNewSsdAccess = false;

        Iterator<Block> iterator = ssdHistory.keySet().iterator();
        while (iterator.hasNext()) {
            Block block = iterator.next();
            updateBlockHistory(ssdHistory.get(block).iterator(),time);
            if (block.isMigrate()) {
                continue;
            }

            double f = (double) ssdHistory.get(block).size() / Configuration.getIntProperty("maxFrequencyInterval");
            if (f < frequecy) {
                block.setMigrate(true);
                result.add(block);
            }
        }
        return result;
    }

    public List<Block> getRemoteFromSsd(double time)
    {
        List<Block> result = new ArrayList<>();
        if(!hasNewSsdAccess){
            return result;
        }
        hasNewSsdAccess = false;

        Iterator<Block> iterator = ssdHistory.keySet().iterator();
        while (iterator.hasNext()) {
            Block block = iterator.next();
            updateBlockHistory(ssdHistory.get(block).iterator(), time);
        }
        ValueComparator vc = new ValueComparator(ssdHistory);
        TreeMap<Block, List<Double>> sorted_map = new TreeMap<>(vc);

        double migrateSpace = this.datanode.getSsdStorage().getUsedSize() - this.datanode.getSsdStorage().getCapacity() * Configuration.getDoubleProperty("threshold") / 2;

        Iterator<Block> blockIterator = sorted_map.keySet().iterator();
        while (blockIterator.hasNext() && migrateSpace >= 0){
            Block block = blockIterator.next();
            if(block.isMigrate()){
                continue;
            }
            migrateSpace -= block.getSize();
            block.setMigrate(true);
            result.add(block);
        }
        return result;
    }

    class ValueComparator implements Comparator<Block>{

        private Map<Block, List<Double>> m;
        public ValueComparator(Map<Block, List<Double>> m){
            this.m = m;
        }

        @Override
        public int compare(Block o1, Block o2) {
            if(m.get(o1).size() < m.get(o2).size()){
                return -1;
            }else {
                return 1;
            }
        }
    }

    private void updateBlockHistory(Iterator<Double> iterator, double time)
    {
        while (iterator.hasNext()) {
            double t = iterator.next();
            if (time - t > Configuration.getIntProperty("maxFrequencyInterval")) {
                iterator.remove();
            } else {
                break;
            }
        }
    }
}
