package org.bjut.hdfssim.models.HDFS;

import org.bjut.hdfssim.*;
import org.bjut.hdfssim.config.CreateConfig;
import org.bjut.hdfssim.models.Request.Request;
import org.bjut.hdfssim.util.Helper;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSim;
import org.junit.Before;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;

public class hdfssimTest {
    private static Namenode namenode;
    private static HDFSDatacenter datacenter;
    private static HDFSBroker hdfsBroker;
    @Before
    public void setUp() throws Exception {
        // First step: Initialize the CloudSim package. It should be called before creating any entities.
        int num_user = 1; // number of cloud users
        Calendar calendar = Calendar.getInstance(); // Calendar whose fields have been initialized with the
        // current date and time.
        boolean trace_flag = false; // trace events
        CloudSim.init(num_user, calendar, trace_flag);
    }

    @Test
    public void testCreateConfig() throws Exception {
        CreateConfig.excute(Helper.getConfigPath("HDFSConfig"),100,100);
    }

    @Test
    public void testDefault() throws Exception {
        Log.printLine("Starting Default...");
        try {
            String name = "HDFSConfig";
            init(name);
            excuteDefault(name);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }
    }

    @Test
    public void testLoad() throws Exception {
        Log.printLine("Starting Default...");
        try {
            String name = "HDFSConfig";
            init(name);
            excuteLoad(name);
        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Unwanted errors happen");
        }

    }

    private static void init(String name) {
        namenode = new Namenode();
        datacenter = new HDFSDatacenter("HDFSDatacenter", namenode);
        try {
            hdfsBroker = new HDFSBroker("brocker", datacenter);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void reset(String name) {
        namenode.initFromConfigFile(Helper.getConfigPath(name));
        datacenter.setHostList();
        hdfsBroker.restRequestList();
    }

    private static void excuteDefault(String name) {
        String path = Helper.getConfigPath(name);
        reset(name);
        datacenter.setPolicy(new DefaultDatanodeAllocationPolicy());
        List<Request> requestList = namenode.getRquestListFromConfigFile(path);
        hdfsBroker.submitRequests(requestList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();
        String resultPath = Configuration.getBasePath() + name + "_1.csv";
        Helper.saveResult(requestList, resultPath);
    }

    private static void excuteLoad(String name) {
        String path = Helper.getConfigPath(name);
        reset(name);
        datacenter.setPolicy(new LoadDatanodeAllocationPolicy());
        List<Request> requestList = namenode.getRquestListFromConfigFile(path);
        hdfsBroker.submitRequests(requestList);
        CloudSim.startSimulation();
        CloudSim.stopSimulation();
        String resultPath = Configuration.getBasePath() + name + "_2.csv";
        Helper.saveResult(requestList, resultPath);
    }
}