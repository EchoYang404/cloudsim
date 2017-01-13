package org.bjut.hdfssim.experiment;

import org.cloudbus.cloudsim.Log;

public class Default {
    public static void main(String[] args) {
        if(args.length == 0) {
            Log.printLine("There is no args!");
            return;
        }
        String path = args[0];
    }
}
