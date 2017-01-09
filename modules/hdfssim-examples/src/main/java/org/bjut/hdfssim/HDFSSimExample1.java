package org.bjut.hdfssim;

import org.bjut.hdfssim.config.CreateConfig;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.Calendar;
import java.util.List;

public class HDFSSimExample1 {
    public static void main(String[] args) {
        Log.printLine("Starting HDFSSimExample1...");
        try {
            // First step: Initialize the CloudSim package. It should be called before creating any entities.
            int num_user = 1; // number of cloud users
            Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the
            // current date and time.
            boolean trace_flag = false; // trace events
            CloudSim.init(num_user, calendar, trace_flag);

            String path = Configuration.getBasePath() + "HDFSConfig.json";
            String resultPath = Configuration.getBasePath() + "result2.csv";
            //CreateConfig.excute(path);
            // First step : Create namenode
            Namenode namenode = new Namenode();
            // Second step : Create datanodes from Config Files
            namenode.setDatanodesFromConfigFile(path);
            // Third step : Create HFiles from Config Files and upload to namenode
            namenode.setHFilesFromConfigFile(path);

//            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
//                    DefaultDatanodeAllocationPolicy());

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
             LoadDatanodeAllocationPolicy());

            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            //List<Request> requestList = namenode.createRequestListByRandom(10);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);

            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();

            CloudSim.stopSimulation();

            Helper.saveResult(requestList, resultPath);

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }
}
