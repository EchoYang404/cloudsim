package org.bjut.hdfssim.models.Request;

import org.bjut.hdfssim.Block;
import org.bjut.hdfssim.HDFSHost;
import org.bjut.hdfssim.models.HDFS.Datanode;

public class MigrateCloudlet extends HCloudlet {
    private Datanode destNode;
    private Datanode srcNode;

    private int toType;

    public MigrateCloudlet(int blockId, Block block, Datanode srcNode, Datanode destNode, int toType) {
        super(blockId, block.gethFile().getReplicaListById(blockId), block.getSize());
        this.resetCpuStageLength();

        this.setBlock(block);
        this.srcNode = srcNode;
        this.destNode = destNode;
        this.toType = toType;
    }

    public Datanode getDestNode() {
        return destNode;
    }

    public Datanode getSrcNode() {
        return srcNode;
    }

    @Override
    public void allocateHost(HDFSHost host) {
        this.setHost(host);
        if (this.getHost().getDatanode() == this.destNode) {
            this.resetBwStageLength();
        } else if (this.getHost().getDatanode().getRackId() != destNode.getRackId()) {
            this.setMaxStage(HCloudlet.NET);
            this.setNetStage(new Stage(this.getBlockList().get(0).getSize()));
        }
    }

    @Override
    protected void stop() {
    }
}
