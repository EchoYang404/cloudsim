package org.bjut.hdfssim.util;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.models.HDFS.DatanodeType;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CreateConfig {

    public static void main(String[] args)
    {
        List<DatanodeType> datanodeTypeList = new ArrayList<>();
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));
        datanodeTypeList.add(new DatanodeType(3200,16,8000,1024000,512000));

        Yaml yaml = new Yaml();
        String path = Configuration.getBasePath() + "DatanodeType.yaml";
        try {
            yaml.dump(datanodeTypeList,new FileWriter(path));

//            Iterable<Object> obj = yaml.loadAll(new FileInputStream(path));
//            for(Object o : obj)
//            {
//                Map<String, Object> map = (Map<String, Object>) obj;
//            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
