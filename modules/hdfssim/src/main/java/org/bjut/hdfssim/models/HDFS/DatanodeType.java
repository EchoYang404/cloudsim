package org.bjut.hdfssim.models.HDFS;

public class DatanodeType {
    //public int id;
    public double mips;
    public int cores;
    public int ram;
    public double hddCapacity;
    public double ssdCapacity;

    public DatanodeType()
    {

    }

    public DatanodeType(double mips, int cores, int ram, double hddCapacity, double ssdCapacity)
    {
        this.mips = mips;
        this.cores = cores;
        this.ram = ram;
        this.hddCapacity = hddCapacity;
        this.ssdCapacity = ssdCapacity;
    }
}
