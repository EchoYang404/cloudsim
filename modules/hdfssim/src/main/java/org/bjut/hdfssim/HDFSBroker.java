package org.bjut.hdfssim;

import com.sun.org.apache.regexp.internal.RE;
import org.bjut.hdfssim.models.Request.Request;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.core.CloudSimTags;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;

import java.util.ArrayList;
import java.util.List;

public class HDFSBroker extends DatacenterBroker{
    private List<Request> requestList;
    private HDFSDatacenter datacenter;

    /**
     * Created a new DatacenterBroker object.
     *
     * @param name name to be associated with this entity (as required by {@link SimEntity} class)
     * @throws Exception the exception
     * @pre name != null
     * @post $none
     */
    public HDFSBroker(String name, HDFSDatacenter datacenter) throws Exception {
        super(name);
        this.requestList = new ArrayList<>();
        this.datacenter = datacenter;
    }

    public void submitRequests(List<Request> requestList)
    {
        for(Request r : requestList)
        {
            r.setBroker(this);
            this.requestList.add(r);
        }
    }


    @Override
    public void startEntity() {
        schedule(getId(), 0, CloudSimTags.SubmitRequests);
    }

    @Override
    public void processEvent(SimEvent ev) {
        switch (ev.getTag()) {
            // 提交Requests
            case CloudSimTags.SubmitRequests:
                processSubmitRequests();
                break;
        }
    }

    protected void processSubmitRequests()
    {
        for (Request request : requestList)
        {
            send(datacenter.getId(), request.getSubmitTime(), CloudSimTags.RequestCreate, request);
        }
    }

    public void restRequestList()
    {
        this.requestList.clear();
    }
}
