package org.bjut.hdfssim.config;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.config.HDFSConfig;
import org.bjut.hdfssim.config.RackConfig;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.util.Helper;

public class ConfigHelper {
    public static void createRackConfig(double blockSize, int replicaCount){
        HDFSConfig config = new HDFSConfig();
        config.setBlockSize(blockSize);
        config.setReplicaCount(replicaCount);
        // Config racks and write to json file
        configRacks(config);
        config.writeToFile(Helper.getConfigPath(Configuration.getStringProperty("rackPath")+"rackConfig_" + config.getBlockSize() + "_" + config.getReplicaCount()));
    }

    public static void createExConfig(String path, String name, int requestNum) {
        HDFSConfig config = new HDFSConfig();
        // Create namenode
        Namenode namenode = new Namenode();

        // Create datanodes from Config File
        namenode.createDatanodeFromConfigFile(path);
        config.setDatanodeConfigList(namenode);

        // Create HFiles by random and write to json file
        namenode.createHFileByRadom(Configuration.getIntProperty("fileNum"), Configuration.getIntProperty("minSize"),
                Configuration.getIntProperty("maxSize"));
        config.setHFileConfigList(namenode);

        // Create request by random and write to json file
        config.setRequestConfigList(namenode, requestNum);
        config.writeToFile(Helper.getConfigPath(Configuration.getStringProperty("exPath") + "ex_" + name + "_" + requestNum + "_" + System.currentTimeMillis()));
    }


    private static void configRacks(HDFSConfig config) {
        config.getDatanodeTypeList().add(new DatanodeType(3200, 16, 8192, 1000, 1024000, 100, 512000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(3000, 14, 7168, 1000, 1024000, 100, 512000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(2800, 12, 6144, 1000, 1024000, 100, 512000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(2600, 10, 5120, 1000, 1024000, 100, 512000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(2400, 8, 4096, 1000, 512000, 100, 256000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(2200, 6, 3072, 1000, 512000, 100, 256000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(2000, 4, 2048, 1000, 512000, 100, 256000, 300));
        config.getDatanodeTypeList().add(new DatanodeType(1800, 2, 1024, 1000, 512000, 100, 256000, 300));

        config.getRackConfigList().add(new RackConfig(0, 1, 5));
        config.getRackConfigList().add(new RackConfig(0, 3, 5));
        config.getRackConfigList().add(new RackConfig(0, 5, 5));
        config.getRackConfigList().add(new RackConfig(0, 7, 5));

        config.getRackConfigList().add(new RackConfig(1, 0, 5));
        config.getRackConfigList().add(new RackConfig(1, 2, 5));
        config.getRackConfigList().add(new RackConfig(1, 4, 5));
        config.getRackConfigList().add(new RackConfig(1, 6, 5));

        config.getRackConfigList().add(new RackConfig(2, 1, 5));
        config.getRackConfigList().add(new RackConfig(2, 3, 5));
        config.getRackConfigList().add(new RackConfig(2, 5, 5));
        config.getRackConfigList().add(new RackConfig(2, 7, 5));

        config.getRackConfigList().add(new RackConfig(3, 0, 5));
        config.getRackConfigList().add(new RackConfig(3, 2, 5));
        config.getRackConfigList().add(new RackConfig(3, 4, 5));
        config.getRackConfigList().add(new RackConfig(3, 6, 5));

        config.getRackConfigList().add(new RackConfig(4, 1, 5));
        config.getRackConfigList().add(new RackConfig(4, 3, 5));
        config.getRackConfigList().add(new RackConfig(4, 5, 5));
        config.getRackConfigList().add(new RackConfig(4, 7, 5));
    }
}
