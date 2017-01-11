package org.bjut.hdfssim;

import org.bjut.hdfssim.config.CreateConfig;
import org.bjut.hdfssim.models.HDFS.Namenode;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.util.Helper;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

import static org.junit.Assert.*;

public class HdfssimTest {
    private String name = "HDFSConfig";
    private Namenode namenode;

    public void init()
    {
        String path = Helper.getConfigPath(name);
        // First step : Create namenode
        namenode = new Namenode();
        // Second step : Create datanodes from Config Files
        namenode.setDatanodesFromConfigFile(path);
        // Third step : Create HFiles from Config Files and upload to namenode
        namenode.setHFilesFromConfigFile(path);
    }

    @Test
    public void testCreateConfig() throws Exception {
        CreateConfig.excute(Helper.getConfigPath(name),100,100);
    }

    @Test
    public void testDefault() throws Exception {
        //CreateConfig.excute(Helper.getConfigPath(name),100,100);
        Log.printLine("Starting Default...");
        init();
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            String path = Helper.getConfigPath(name);
            String resultPath = Configuration.getBasePath() + name + "_1.csv";

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
                    DefaultDatanodeAllocationPolicy(), false);
            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);
            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();
            Helper.printStorageAccessTime(namenode);
            Helper.saveResult(requestList, resultPath);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    @Test
    public void testLoad() throws Exception {
        Log.printLine("Starting Load...");
        init();
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            String path = Helper.getConfigPath(name);
            String resultPath = Configuration.getBasePath() + name + "_2.csv";

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
                    LoadDatanodeAllocationPolicy(), false);
            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);
            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();
            Helper.printStorageAccessTime(namenode);
            Helper.saveResult(requestList, resultPath);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    @Test
    public void testDefaultWithMigrate() throws Exception {
        //CreateConfig.excute(Helper.getConfigPath(name),100,100);
        Log.printLine("Starting Default...");
        init();
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            String path = Helper.getConfigPath(name);
            String resultPath = Configuration.getBasePath() + name + "_3.csv";

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
                    DefaultDatanodeAllocationPolicy(), true);
            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);
            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();
            Helper.printStorageAccessTime(namenode);
            Helper.saveResult(requestList, resultPath);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    @Test
    public void testLoadWithMigrate() throws Exception {
        Log.printLine("Starting Load...");
        init();
        try {
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            String path = Helper.getConfigPath(name);
            String resultPath = Configuration.getBasePath() + name + "_4.csv";

            HDFSDatacenter datacenter = new HDFSDatacenter("HDFSDatacenter", namenode, new
                    LoadDatanodeAllocationPolicy(), true);
            HDFSBroker hdfsBroker = new HDFSBroker("brocker", datacenter);

            List<Request> requestList = namenode.getRquestListFromConfigFile(path);
            hdfsBroker.submitRequests(requestList);

            CloudSim.startSimulation();
            CloudSim.stopSimulation();
            Helper.printStorageAccessTime(namenode);
            Helper.saveResult(requestList, resultPath);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }
}