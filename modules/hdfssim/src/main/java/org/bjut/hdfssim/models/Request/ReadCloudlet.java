package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDFSHost;

import java.util.List;

public class ReadCloudlet extends HCloudlet {

    private Request request;

    public ReadCloudlet(int blockId, List<Block> blockList, Request request) {
        super(blockId, blockList, blockList.get(0).getSize());
        this.request = request;
    }

    public Request getRequest() {
        return request;
    }

    @Override
    public void allocateHost(HDFSHost host) {
        this.setHost(host);
        if (this.getHost().getDatanode() == this.request.getAddr()) {
            this.resetBwStageLength();
        } else if (this.getHost().getDatanode().getRackId() != this.request.getAddr().getRackId()) {
            this.setMaxStage(HCloudlet.NET);
            this.setNetStage(new Stage(this.getBlockList().get(0).getSize()));
        }
    }
}
