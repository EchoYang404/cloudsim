package org.bjut.hdfssim.experiment;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.config.ConfigHelper;
import org.bjut.hdfssim.util.Id;

import java.io.File;
import java.util.Arrays;

public class CreateExConfig {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println(Arrays.toString(args));
            System.out.println("There is no args!");
            return;
        }
        int requestNum = Integer.valueOf(args[0]);
        int exTime = Integer.valueOf(args[1]);
//        int requestNum = 100;
//        int exTime = 1;

        File file = new File(Configuration.getBasePath() + Configuration.getStringProperty("rackPath"));
        File flist[] = file.listFiles();
        if (flist == null || flist.length == 0) {
            System.out.println("There is no rack config files!");
            return;
        }

        for (File f : flist) {
            String name = f.getName();
            name = name.substring(0,name.lastIndexOf("."));
            name = name.substring(name.indexOf("_") + 1,name.length());
            for (int i = 0; i < exTime; i++) {
                ConfigHelper.createExConfig(f.getPath(),name,requestNum);
                Id.reset();
            }
        }
    }
}
