package org.bjut.hdfssim;

import org.bjut.hdfssim.util.Id;

import java.util.ArrayList;
import java.util.List;

public class Block {
    private int id;
    private double size;
    private List<Storage> storageList;

    public Block(double size)
    {
        this.id = Id.pollId(Block.class);
        this.size = size;
        this.storageList = new ArrayList<>();
    }

    public Block(int id, double size)
    {
        this.id = id;
        this.size = size;
        this.storageList = new ArrayList<>();
    }

    public int getId()
    {
        return id;
    }

    public double getSize() {
        return size;
    }

    public boolean addStorage(Storage storage)
    {
        if(Configuration.getIntProperty("replicaCount") > storageList.size())
        {
            storageList.add(storage);
            return true;
        }
        return false;
    }

    public boolean deleteStorage(Storage storage)
    {
        storageList.remove(storage);
        return true;
    }


}
