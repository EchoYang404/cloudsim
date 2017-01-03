package org.bjut.hdfssim.provisioners;

public class BwProvisionerForHDFS {
    /** The total bandwidth capacity from the HDFSHost. */
    private double bw;
    private int currentNum = 0;
    //TODO 添加任务列表，当currentNum改变时，应当更改任务列表中的所有任务的speed
    public BwProvisionerForHDFS(double bw)
    {
        this.bw = bw;
    }
}
