package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.Configuration;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public class ReadCloudlet extends HCloudlet {
    private int blockId;
    private List<Block> blockList;
    private Request request;

    public ReadCloudlet(int blockId, List<Block> blockList, Request request) {
        super(blockList.get(0).getSize());
        this.blockId = blockId;
        this.blockList = blockList;
        this.request = request;
    }

    public List<Block> getBlockList() {
        return blockList;
    }

    public int getBlockId() {
        return blockId;
    }

    public Request getRequest() {
        return request;
    }

    @Override
    public void allocateHost(HDFSHost host) {
        this.setHost(host);
        if (this.getHost().getDatanode().getRackId() != this.request.getAddr().getRackId()) {
            this.setMaxStage(ReadCloudlet.NET);
            this.setNetStage(new Stage(blockList.get(0).getSize()));
        }
    }
}
