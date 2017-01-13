package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.Storage;
import org.bjut.hdfssim.models.Request.MigrateCloudlet;

import java.util.*;

public class Migrationer {
    private Namenode namenode;
    private double frequency;
    private List<MigrateCloudlet> historyCloudletList;
    private List<MigrateCloudlet> waitList;
    private double lastDownMigrateTime;

    public Migrationer(Namenode namenode) {
        this.namenode = namenode;
        this.historyCloudletList = new ArrayList<>();
        this.waitList = new ArrayList<>();
        this.frequency = Configuration.getDoubleProperty("frequency");
        this.lastDownMigrateTime = 0;
    }

    public List<MigrateCloudlet> check(double time) {
        checkMigrate(time);
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        Iterator<MigrateCloudlet> iterator = waitList.iterator();
        while (iterator.hasNext()) {
            MigrateCloudlet mc = iterator.next();
            if (mc.getBlock().getCurrentNum() == 0) {
                cloudletList.add(mc);
                iterator.remove();
            }
        }
        return cloudletList;
    }

    private void checkMigrate(double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                if (datanode.getSSDUtilization() > Configuration.getDoubleProperty("threshold")) {
                    // 执行跨节点迁移
                    cloudletList.addAll(remoteMigrate(datanode, time));
                } else {
                    // 执行上迁
                    cloudletList.addAll(upMigrate(datanode, time));
                    // 执行下迁
                    if(time - lastDownMigrateTime > Configuration.getDoubleProperty("downMigrateInterval") && datanode.getSSDUtilization() > Configuration.getDoubleProperty("threshold") * 0.9)
                    {
                        cloudletList.addAll(downMigrate(datanode,time));
                    }
                }
            }
        }
        historyCloudletList.addAll(cloudletList);
        waitList.addAll(cloudletList);
    }

    // 上迁
    private List<MigrateCloudlet> upMigrate(Datanode datanode, double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        List<Block> blockList = datanode.getInfo().getGreaterFromHdd(frequency, time);
        Iterator<Block> iterator = blockList.iterator();
        while (iterator.hasNext()){
            Block block = iterator.next();
            cloudletList.add(createMigrateCloudlet(block,datanode,Storage.SSD));
        }
        return cloudletList;
    }

    // 下迁
    private List<MigrateCloudlet> downMigrate(Datanode datanode, double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        List<Block> blockList = datanode.getInfo().getLessFromSsd(frequency, time);
        Iterator<Block> iterator = blockList.iterator();
        while (iterator.hasNext()){
            Block block = iterator.next();
            cloudletList.add(createMigrateCloudlet(block,datanode,Storage.HDD));
        }
        return cloudletList;
    }

    private MigrateCloudlet createMigrateCloudlet(Block block, Datanode datanode, int toType)
    {
        block.gethFile().addBlock(block.getId(), block.getSize(), datanode, toType, block.getDatanode().deleteBlock(block));
        return new MigrateCloudlet(block.getId(), block, datanode, toType);
    }

    // 跨节点迁移
    private List<MigrateCloudlet> remoteMigrate(Datanode datanode, double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        List<Block> blockList = datanode.getInfo().getRemoteFromSsd(time);
        if(blockList.isEmpty()){
            return cloudletList;
        }
        int particleNum = Configuration.getIntProperty("particleNum");
        int iterations = Configuration.getIntProperty("iterations");

        // 初始化粒子群
        List<Particle> particles = new ArrayList<>(particleNum);
        List<Datanode> gBest = new ArrayList<>();
        double gBestFitValue = Double.MAX_VALUE;
        // List中，下标0为当前Datanode的SSD使用量，1为SSD总量，2为计算量
        Map<Datanode,List<Double>> datanodeSsdUsage = new HashMap<>();
        double avgUsage = getDatanodeSsdUsage(datanodeSsdUsage, datanode, blockList);
        for(int i = 0; i < particleNum; i++){
            particles.add(new Particle(blockList,datanodeSsdUsage,avgUsage));
        }
        // 计算所有粒子的适应度值，更新自身的最佳位置与集群位置
        for(int i = 0; i < particleNum; i++){
            double tmp = particles.get(i).getFitValue();
            if(gBestFitValue > tmp){
                gBestFitValue = tmp;
                gBest.clear();
                gBest.addAll(particles.get(i).getPosition());
            }
        }

        // 迭代计算
        for(int i = 1; i < iterations; i++){
            List<Datanode> preGBest = new ArrayList<>();
            preGBest.addAll(gBest);
            double pMutation = 1.0/(1.0+Math.exp(0-(double)i/iterations));
            for(int j = 0; j < particleNum; j++){
                particles.get(j).updatePosition(preGBest,pMutation);
                double tmp = particles.get(j).getFitValue();
                if(gBestFitValue > tmp){
                    gBestFitValue = tmp;
                    gBest.clear();
                    gBest.addAll(particles.get(j).getPosition());
                }
            }
        }

        // 根据计算结果创建MigrateCloudlet
        for(int i = 0; i < blockList.size();i++){
            cloudletList.add(createMigrateCloudlet(blockList.get(i), gBest.get(i), Storage.SSD));
        }
        return cloudletList;
    }

    private class Particle implements Cloneable{
        private List<Datanode> position;
        private Map<Datanode,List<Double>> ssdUsages;
        private double avgUsage;
        private List<Block> blockList;
        private double fitValue;

        private List<Datanode> pBest;
        private double bestFitValue;

        public Particle(List<Block> blockList, Map<Datanode,List<Double>> ssdUsages, double avgUsage){
            position = new ArrayList<>();
            pBest = new ArrayList<>();
            this.ssdUsages = ssdUsages;
            this.blockList = blockList;
            this.avgUsage = avgUsage;

            for(int i = 0; i < blockList.size();i++){
                position.add(getAvailableDatanode(blockList.get(i)));
            }
            pBest.addAll(position);
            this.fitValue = calFitValue(position);
            this.bestFitValue = this.fitValue;
        }

        public void updatePosition(List<Datanode> gBest, double pMutation){
            List<Datanode> p1 = new ArrayList<>();
            List<Datanode> p2 = new ArrayList<>();
            p1.addAll(this.position);
            double c1 = Configuration.getDoubleProperty("c1");
            double c2 = Configuration.getDoubleProperty("c2");
            double pChange = Configuration.getDoubleProperty("pChange");
            Random random = new Random();
            if(c1*random.nextDouble() > c2 * random.nextDouble()){
                p2.addAll(this.pBest);
            }else{
                p2.addAll(gBest);
            }
            int num = position.size();
            for(int i = 0; i < num; i++){
                if(random.nextDouble() >= pChange){
                    Datanode tmp = p1.get(i);
                    p1.set(i,p2.get(i));
                    p2.set(i,tmp);
                }
                if(random.nextDouble() >= pMutation){
                    p1.set(i,getAvailableDatanode(blockList.get(i)));
                }
                if(random.nextDouble() >= pMutation){
                    p2.set(i,getAvailableDatanode(blockList.get(i)));
                }
            }

            double fit1 = calFitValue(p1);
            double fit2 = calFitValue(p2);
            this.position.clear();
            if(fit1 < fit2){
                this.position.addAll(p1);
                this.fitValue = fit1;
            }else{
                this.position.addAll(p2);
                this.fitValue = fit2;
            }

            if(this.fitValue < this.bestFitValue){
                this.pBest.clear();
                this.pBest.addAll(this.position);
                this.bestFitValue = this.fitValue;
            }
        }

        private double calFitValue(List<Datanode> p) {
            // TODO 计算适应度值
            double firstPart = 0, secondPart = 0;
            // 更新迁移后状态
            for (int i = 0; i < p.size(); i++) {
                double preUsage = ssdUsages.get(p.get(i)).get(0);
                ssdUsages.get(p.get(i)).set(0,preUsage+blockList.get(i).getSize());
                ssdUsages.get(p.get(i)).set(2,Math.pow(avgUsage - ssdUsages.get(p.get(i)).get(0)/ ssdUsages.get(p.get(i)).get(1),2));
                secondPart += getChangeLevel(blockList.get(i), p.get(i));
            }
            Iterator<List<Double>> listIterator = ssdUsages.values().iterator();
            while (listIterator.hasNext()){
                List<Double> list = listIterator.next();
                firstPart += list.get(2);
            }
            firstPart /= ssdUsages.size();
            secondPart/= (8 * p.size());
            // 恢复原状态
            for (int i = 0; i < p.size(); i++) {
                double preUsage = ssdUsages.get(p.get(i)).get(0);
                ssdUsages.get(p.get(i)).set(0,preUsage-blockList.get(i).getSize());
                ssdUsages.get(p.get(i)).set(2,Math.pow(avgUsage - ssdUsages.get(p.get(i)).get(0)/ ssdUsages.get(p.get(i)).get(1),2));
            }

            return firstPart + secondPart;
        }

        public List<Datanode> getPosition() {
            return position;
        }

        public double getFitValue() {
            return fitValue;
        }

        @Override
        protected Particle clone(){
            Particle p = null;
            try {
                p = (Particle)super.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            p.position.clear();
            p.position.addAll(this.position);
            return p;
        }
    }

    private double getDatanodeSsdUsage(Map<Datanode, List<Double>> usage, Datanode datanode, List<Block> blockList){
        double totalUsage = 0, totalSize = 0;
        Iterator<List<Datanode>> rackIterator = namenode.getDatanodeList().values().iterator();
        while (rackIterator.hasNext()){
            Iterator<Datanode> iterator = rackIterator.next().iterator();
            while (iterator.hasNext()){
                Datanode next = iterator.next();
                List<Double> ssdUage = new ArrayList<>();
                ssdUage.add(next.getSsdStorage().getUsedSize());
                ssdUage.add(next.getSsdStorage().getCapacity());
                totalUsage += ssdUage.get(0);
                totalSize += ssdUage.get(1);
                usage.put(next,ssdUage);
            }
        }
        double avgUsage = totalUsage/totalSize;
        Iterator<List<Double>> listIterator = usage.values().iterator();
        while (listIterator.hasNext()){
            List<Double> list = listIterator.next();
            list.add(Math.pow(avgUsage - list.get(0)/list.get(1),2));
        }

        Iterator<Block> blockIterator = blockList.iterator();
        double migrateSize = 0;
        while (blockIterator.hasNext()){
            Block block = blockIterator.next();
            migrateSize += block.getSize();
        }
        double preUsage = usage.get(datanode).get(0);
        usage.get(datanode).set(0,preUsage - migrateSize);
        usage.get(datanode).set(2,Math.pow(avgUsage - usage.get(datanode).get(0)/usage.get(datanode).get(1),2));
        return avgUsage;
    }

    private Datanode getAvailableDatanode(Block block){
        Random random = new Random();

        int num = block.getThisRackBlockNum();
        int rackId = block.getDatanode().getRackId();
        int totalRack = namenode.getDatanodeList().size();
        Set<Integer> racks = block.getAllRacks();

        int destRackId = -1;
        if(num == 1){
            Set<Integer> unavailableRacks = new HashSet<>();
            unavailableRacks.addAll(racks);
            unavailableRacks.remove(rackId);
            while (destRackId == -1){
                int tmpRackId = random.nextInt(totalRack);
                if(!unavailableRacks.contains(tmpRackId)){
                    destRackId = tmpRackId;
                }
            }
        }else{
            Set<Integer> availableRacks = new HashSet<>();
            availableRacks.addAll(racks);
            while (destRackId == -1){
                int tmpRackId = random.nextInt(totalRack);
                if(availableRacks.contains(tmpRackId)){
                    destRackId = tmpRackId;
                }
            }
        }

        while (true){
            int i = random.nextInt(namenode.getDatanodeList().get(destRackId).size());
            Datanode destNode = namenode.getDatanodeList().get(destRackId).get(i);
            if(!destNode.isContainBlock(block.getId())){
                return destNode;
            }
        }
    }

    private int getChangeLevel(Block block, Datanode datanode){
        if(block.getDatanode().getRackId() == datanode.getRackId()){
            return 0;
        }
        Set<Integer> rackSet = block.getAllRacks();
        if(rackSet.contains(datanode.getRackId())){
            return 4;
        }
        return 8;
    }
}
