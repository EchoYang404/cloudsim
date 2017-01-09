package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.models.Request.Request;

import java.util.List;

public interface DatanodeAllocationPolicy {
    public Datanode getDatanode(List<Block> blockList, Datanode addr);
}
