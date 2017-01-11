package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.Storage;
import org.bjut.hdfssim.models.Request.MigrateCloudlet;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Migrationer {
    private Namenode namenode;
    private double frequency;
    private List<MigrateCloudlet> historyCloudletList;

    public Migrationer(Namenode namenode) {
        this.namenode = namenode;
        this.historyCloudletList = new ArrayList<>();
        this.frequency = Configuration.getDoubleProperty("frequency");
    }

    public List<MigrateCloudlet> check(double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                if (datanode.getSSDUtilization() > Configuration.getDoubleProperty("threshold")) {
                    cloudletList.addAll(remoteMigrate(datanode));
                } else {
                    cloudletList.addAll(migrate(datanode, time));
                }
            }
        }
        historyCloudletList.addAll(cloudletList);
        return cloudletList;
    }

    public List<MigrateCloudlet> migrate(Datanode datanode, double time) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        // 上迁
        Iterator<Block> blockIterator = datanode.getInfo().getGreaterFromHdd(frequency, time).iterator();

        while (blockIterator.hasNext()) {
            Block block = blockIterator.next();
            block.gethFile().addBlock(block.getId(), block.getSize(), datanode, Storage.SSD, datanode.getInfo()
                    .deleteHddBlock(block));

            MigrateCloudlet mc = new MigrateCloudlet(block.getId(), block, block.getStorage().getDatanode(), Storage
                    .SSD);
            cloudletList.add(mc);
        }

        // TODO 下迁
        return cloudletList;
    }

    public List<MigrateCloudlet> remoteMigrate(Datanode datanode) {
        List<MigrateCloudlet> cloudletList = new ArrayList<>();
        // TODO 粒子群
        return cloudletList;
    }
}
