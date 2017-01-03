package org.bjut.hdfssim.provisioners;

public class StorageProvisionerForHDFS {
    private double maxTransferRate;
    private int currentNum = 0;
    //TODO 添加任务列表，当currentNum改变时，应当更改任务列表中的所有任务的speed
    public StorageProvisionerForHDFS(double maxTransferRate)
    {
        this.maxTransferRate = maxTransferRate;
    }
}
