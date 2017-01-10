package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public class ReadCloudlet extends HCloudlet {
    private List<Block> blockList;
    private Request request;

    public ReadCloudlet(int blockId, List<Block> blockList, Request request) {
        super(blockId,blockList.get(0).getSize());

        this.blockList = blockList;
        this.request = request;
    }

    public List<Block> getBlockList() {
        return blockList;
    }



    public Request getRequest() {
        return request;
    }

    @Override
    public void allocateHost(HDFSHost host) {
        this.setHost(host);
        if (this.getHost().getDatanode().getRackId() != this.request.getAddr().getRackId()) {
            this.setMaxStage(HCloudlet.NET);
            this.setNetStage(new Stage(blockList.get(0).getSize()));
        }
    }
}
