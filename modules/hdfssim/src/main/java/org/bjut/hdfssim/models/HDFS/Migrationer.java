package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Migrationer {
    private Namenode namenode;
    private double maxTime;
    public Migrationer(Namenode namenode)
    {
        this.namenode = namenode;
        this.maxTime = 0;
    }

    public void check()
    {
        Iterator<Map.Entry<Integer, List<Datanode>>> entryIterator = namenode.getDatanodeList().entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<Integer, List<Datanode>> entry = entryIterator.next();
            Iterator<Datanode> datanodeIterator = entry.getValue().iterator();
            while (datanodeIterator.hasNext()) {
                Datanode datanode = datanodeIterator.next();
                if(datanode.getSSDUtilization() > Configuration.getDoubleProperty("Threshold"))
                {
                    remoteMigrate(datanode);
                }
                else
                {
                    migrate(datanode);
                }
            }
        }
    }

    public void remoteMigrate(Datanode datanode)
    {

    }

    public void migrate(Datanode datanode)
    {

    }
}
