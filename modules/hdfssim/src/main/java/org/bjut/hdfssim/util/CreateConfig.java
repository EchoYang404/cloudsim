package org.bjut.hdfssim.util;

import com.google.gson.Gson;
import jdk.nashorn.internal.ir.debug.JSONWriter;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateConfig {

    public static void main(String[] args)
    {
        HDFSConfig config = new HDFSConfig();
        config.datanodeTypeList.add(new DatanodeType(3200,16,8192, 100,1024000,100,512000,300));
        config.datanodeTypeList.add(new DatanodeType(3000,14,7168, 100,1024000,100,512000,300));
        config.datanodeTypeList.add(new DatanodeType(2800,12,6144, 100,1024000,100,512000,300));
        config.datanodeTypeList.add(new DatanodeType(2600,10,5120, 100,1024000,100,512000,300));
        config.datanodeTypeList.add(new DatanodeType(2400,8,4096, 100,512000,100,256000,300));
        config.datanodeTypeList.add(new DatanodeType(2200,6,3072, 100,512000,100,256000,300));
        config.datanodeTypeList.add(new DatanodeType(2000,4,2048, 100,512000,100,256000,300));
        config.datanodeTypeList.add(new DatanodeType(1800,2,1024, 100,512000,100,256000,300));


        config.datanodeConfigList.add(new DatanodeConfig(0,1,5));
        config.datanodeConfigList.add(new DatanodeConfig(0,3,5));
        config.datanodeConfigList.add(new DatanodeConfig(0,5,5));
        config.datanodeConfigList.add(new DatanodeConfig(0,7,5));

        config.datanodeConfigList.add(new DatanodeConfig(1,0,5));
        config.datanodeConfigList.add(new DatanodeConfig(1,2,5));
        config.datanodeConfigList.add(new DatanodeConfig(1,4,5));
        config.datanodeConfigList.add(new DatanodeConfig(1,6,5));

        config.datanodeConfigList.add(new DatanodeConfig(2,1,5));
        config.datanodeConfigList.add(new DatanodeConfig(2,3,5));
        config.datanodeConfigList.add(new DatanodeConfig(2,5,5));
        config.datanodeConfigList.add(new DatanodeConfig(2,7,5));

        config.datanodeConfigList.add(new DatanodeConfig(3,0,5));
        config.datanodeConfigList.add(new DatanodeConfig(3,2,5));
        config.datanodeConfigList.add(new DatanodeConfig(3,4,5));
        config.datanodeConfigList.add(new DatanodeConfig(3,6,5));

        config.datanodeConfigList.add(new DatanodeConfig(4,1,5));
        config.datanodeConfigList.add(new DatanodeConfig(4,3,5));
        config.datanodeConfigList.add(new DatanodeConfig(4,5,5));
        config.datanodeConfigList.add(new DatanodeConfig(4,7,5));

        String path = Configuration.getBasePath() + "HDFSConfig.json";
        Gson gson = new Gson();
        try {
            FileWriter fw = new FileWriter(new File(path));
            fw.write(gson.toJson(config));
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }


//        config.datanodeConfigList.add(new DatanodeConfig(5,2,5));
//        config.datanodeConfigList.add(new DatanodeConfig(5,4,5));
//        config.datanodeConfigList.add(new DatanodeConfig(5,6,5));
//        config.datanodeConfigList.add(new DatanodeConfig(5,8,5));
//
//        config.datanodeConfigList.add(new DatanodeConfig(6,1,5));
//        config.datanodeConfigList.add(new DatanodeConfig(6,3,5));
//        config.datanodeConfigList.add(new DatanodeConfig(6,5,5));
//        config.datanodeConfigList.add(new DatanodeConfig(6,7,5));
//
//        config.datanodeConfigList.add(new DatanodeConfig(7,2,5));
//        config.datanodeConfigList.add(new DatanodeConfig(7,4,5));
//        config.datanodeConfigList.add(new DatanodeConfig(7,6,5));
//        config.datanodeConfigList.add(new DatanodeConfig(7,8,5));
//
//        config.datanodeConfigList.add(new DatanodeConfig(8,1,5));
//        config.datanodeConfigList.add(new DatanodeConfig(8,3,5));
//        config.datanodeConfigList.add(new DatanodeConfig(8,5,5));
//        config.datanodeConfigList.add(new DatanodeConfig(8,7,5));
//
//        config.datanodeConfigList.add(new DatanodeConfig(9,2,5));
//        config.datanodeConfigList.add(new DatanodeConfig(9,4,5));
//        config.datanodeConfigList.add(new DatanodeConfig(9,6,5));
//        config.datanodeConfigList.add(new DatanodeConfig(9,8,5));

    }
}
