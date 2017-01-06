package org.bjut.hdfssim.util;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.bjut.hdfssim.models.HDFS.Namenode;

public class CreateConfig {

    public static void main(String[] args)
    {
        String path = Configuration.getBasePath() + "HDFSConfig.json";
        excute(path);
    }

    public static void excute(String path)
    {
        HDFSConfig config = new HDFSConfig();
        // Create namenode
        Namenode namenode = new Namenode();

        // Config racks and write to json file
        configRacks(config);
        config.writeToFile(path);

        // Create datanodes from Config File and write to json file
        namenode.createDatanodeFromConfigFile(path);
        config.setDatanodeConfigList(namenode);
        config.writeToFile(path);

        // Create HFiles by random and write to json file
        namenode.createHFileByRadom(1000,1024,2048);
        config.setHFileConfigList(namenode);
        config.writeToFile(path);
    }


    private static void configRacks(HDFSConfig config)
    {
        config.getDatanodeTypeList().add(new DatanodeType(3200,16,8192, 100,1024000,100,512000,300));
        config.getDatanodeTypeList().add(new DatanodeType(3000,14,7168, 100,1024000,100,512000,300));
        config.getDatanodeTypeList().add(new DatanodeType(2800,12,6144, 100,1024000,100,512000,300));
        config.getDatanodeTypeList().add(new DatanodeType(2600,10,5120, 100,1024000,100,512000,300));
        config.getDatanodeTypeList().add(new DatanodeType(2400,8,4096, 100,512000,100,256000,300));
        config.getDatanodeTypeList().add(new DatanodeType(2200,6,3072, 100,512000,100,256000,300));
        config.getDatanodeTypeList().add(new DatanodeType(2000,4,2048, 100,512000,100,256000,300));
        config.getDatanodeTypeList().add(new DatanodeType(1800,2,1024, 100,512000,100,256000,300));

        config.getRackConfigList().add(new RackConfig(0,1,5));
        config.getRackConfigList().add(new RackConfig(0,3,5));
        config.getRackConfigList().add(new RackConfig(0,5,5));
        config.getRackConfigList().add(new RackConfig(0,7,5));

        config.getRackConfigList().add(new RackConfig(1,0,5));
        config.getRackConfigList().add(new RackConfig(1,2,5));
        config.getRackConfigList().add(new RackConfig(1,4,5));
        config.getRackConfigList().add(new RackConfig(1,6,5));

        config.getRackConfigList().add(new RackConfig(2,1,5));
        config.getRackConfigList().add(new RackConfig(2,3,5));
        config.getRackConfigList().add(new RackConfig(2,5,5));
        config.getRackConfigList().add(new RackConfig(2,7,5));

        config.getRackConfigList().add(new RackConfig(3,0,5));
        config.getRackConfigList().add(new RackConfig(3,2,5));
        config.getRackConfigList().add(new RackConfig(3,4,5));
        config.getRackConfigList().add(new RackConfig(3,6,5));

        config.getRackConfigList().add(new RackConfig(4,1,5));
        config.getRackConfigList().add(new RackConfig(4,3,5));
        config.getRackConfigList().add(new RackConfig(4,5,5));
        config.getRackConfigList().add(new RackConfig(4,7,5));

        //        config.datanodeConfigList.add(new RackConfig(5,2,5));
//        config.datanodeConfigList.add(new RackConfig(5,4,5));
//        config.datanodeConfigList.add(new RackConfig(5,6,5));
//        config.datanodeConfigList.add(new RackConfig(5,8,5));
//
//        config.datanodeConfigList.add(new RackConfig(6,1,5));
//        config.datanodeConfigList.add(new RackConfig(6,3,5));
//        config.datanodeConfigList.add(new RackConfig(6,5,5));
//        config.datanodeConfigList.add(new RackConfig(6,7,5));
//
//        config.datanodeConfigList.add(new RackConfig(7,2,5));
//        config.datanodeConfigList.add(new RackConfig(7,4,5));
//        config.datanodeConfigList.add(new RackConfig(7,6,5));
//        config.datanodeConfigList.add(new RackConfig(7,8,5));
//
//        config.datanodeConfigList.add(new RackConfig(8,1,5));
//        config.datanodeConfigList.add(new RackConfig(8,3,5));
//        config.datanodeConfigList.add(new RackConfig(8,5,5));
//        config.datanodeConfigList.add(new RackConfig(8,7,5));
//
//        config.datanodeConfigList.add(new RackConfig(9,2,5));
//        config.datanodeConfigList.add(new RackConfig(9,4,5));
//        config.datanodeConfigList.add(new RackConfig(9,6,5));
//        config.datanodeConfigList.add(new RackConfig(9,8,5));
    }
}
