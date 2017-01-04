package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.util.Id;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class HDFSSimExample1 {
    public static void main(String[] args) {
        Log.printLine("Starting HDFSSimExample1...");
        try {
            // First step: Initialize the CloudSim package. It should be called before creating any entities.
            int num_user = 1; // number of cloud users
            Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the current date and time.
            boolean trace_flag = false; // trace events
            CloudSim.init(num_user, calendar, trace_flag);

            // First step : Create namenode
            Namenode namenode = new Namenode();
            // Second step : Create datanodes from Config Files
            namenode.createDatanodeFromConfig(Configuration.getBasePath() + "HDFSConfig.json");
            // Third step : Create files and upload files
            namenode.createHFileByRadom(1000, 1024, 2048);

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new DefaultDatanodeAllocationPolicy());

            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.createRequestListByRandom(10);

            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();

            CloudSim.stopSimulation();

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }
}
