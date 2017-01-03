package org.bjut.hdfssim.models.HDFS;

public class DatanodeType {
    //public int id;
    public double mips; // MillionInstructions/s
    public int cores;
    public int ram; //MB
    public double bw; // MB/s
    public double hddCapacity; // MB
    public double hddMaxTransferRate;// MB/s
    public double ssdCapacity; //MB
    public double ssdMaxTransferRate;// MB/s

    public DatanodeType()
    {

    }

    public DatanodeType(double mips, int cores, int ram, double bw, double hddCapacity, double hddMaxTransferRate, double ssdCapacity, double ssdMaxTransferRate)
    {
        this.mips = mips;
        this.cores = cores;
        this.ram = ram;
        this.bw = bw;
        this.hddCapacity = hddCapacity;
        this.ssdCapacity = ssdCapacity;
        this.ssdMaxTransferRate = ssdMaxTransferRate;
        this.hddMaxTransferRate = hddMaxTransferRate;
    }
}
