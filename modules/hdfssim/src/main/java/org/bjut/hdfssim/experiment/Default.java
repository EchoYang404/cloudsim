package org.bjut.hdfssim.experiment;

import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.DefaultDatanodeAllocationPolicy;
import org.bjut.hdfssim.HDFSBroker;
import org.bjut.hdfssim.HDFSDatacenter;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.util.Helper;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.Calendar;
import java.util.List;

public class Default {
    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("There is no args!");
            return;
        }
        String path = args[0];
        //String path = "D:\\projects\\cloudsim\\modules\\hdfssim\\target\\classes\\ex\\ex_128.0_3_5_1484714596778.json";
        System.out.println(path);
        // First step : Create namenode
        Namenode namenode = new Namenode();
        // Second step : Create datanodes from Config Files
        namenode.setDatanodesFromConfigFile(path);
        // Third step : Create HFiles from Config Files and upload to namenode
        namenode.setHFilesFromConfigFile(path);
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
                    DefaultDatanodeAllocationPolicy(), false);
            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);
            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();
            Helper.saveExResult(namenode, requestList,0);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }
}
