package org.bjut.hdfssim.experiment;

import org.bjut.hdfssim.config.ConfigHelper;

import java.util.Arrays;

public class CreateRackConfig {
    public static void main(String[] args) {
        if(args.length != 2) {
            System.out.println(Arrays.toString(args));
            System.out.println("There is no args!");
            return;
        }
        ConfigHelper.createRackConfig(Double.valueOf(args[0]), Integer.valueOf(args[1]));
    }
}
