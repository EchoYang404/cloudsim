package org.bjut.hdfssim.models.Request;

import org.cloudbus.cloudsim.core.SimEvent;

public class Request extends SimEvent {
    public int id;
    public double submissionTime;
    public Job job;
}
