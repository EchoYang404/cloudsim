package org.bjut.hdfssim.provisioners;

public class PeProvisionerForHDFS {
    /** The total mips capacity of the PE. */
    private double mips;
    private int currentNum = 0;
    //TODO 添加任务列表，当currentNum改变时，应当更改任务列表中的所有任务的speed
    public PeProvisionerForHDFS(double mips)
    {
        this.mips = mips;
    }
}
