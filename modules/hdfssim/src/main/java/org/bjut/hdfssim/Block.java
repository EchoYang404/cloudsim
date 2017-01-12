package org.bjut.hdfssim;

import org.bjut.hdfssim.models.HDFS.Datanode;
import org.bjut.hdfssim.util.Id;

import java.io.Serializable;
import java.util.*;

public class Block implements Serializable {
    private HFile hFile;
    private int id;
    private double size;
    private int currentNum;
    private boolean isMigrate;
    private Storage storage;

    public Block(HFile hFile, int id, double size) {
        this.hFile = hFile;
        this.id = id;
        this.size = size;
        this.currentNum = 0;
        this.isMigrate = false;
        this.storage = null;
    }

    public double getSize() {
        return size;
    }

    public int getId() {
        return id;
    }

    public void setStorage(Storage storage) {
        this.storage = storage;
    }

    public Storage getStorage() {
        return storage;
    }

    public Datanode getDatanode(){
        return this.getStorage().getDatanode();
    }

    public Set<Integer> getAllRacks() {
        Iterator<Block> blockIterator = hFile.getReplicaListById(this.id).iterator();
        Set<Integer> racks = new HashSet<>();
        while (blockIterator.hasNext()) {
            racks.add(blockIterator.next().getDatanode().getRackId());
        }
        return racks;
    }

    public int getThisRackBlockNum()
    {
        Iterator<Block> blockIterator = hFile.getReplicaListById(this.id).iterator();
        int num = 0;
        while (blockIterator.hasNext()) {
            Block block = blockIterator.next();
            if(block.isMigrate()){
                continue;
            }
            else {
                if(this.getDatanode().getRackId() == block.getDatanode().getRackId()){
                    num++;
                }
            }
        }
        return num;
    }

    public HFile gethFile() {
        return hFile;
    }

    public void access() {
        this.currentNum++;
    }

    public void finishAccess(){
        this.currentNum--;
        if(this.currentNum == 0 && this.isMigrate())
        {
            this.storage.deleteBlock(this);
            this.hFile.getReplicaListById(this.getId()).remove(this);
        }
    }

    public int getCurrentNum() {
        return currentNum;
    }

    public void setMigrate(boolean migrate) {
        isMigrate = migrate;
    }

    public boolean isMigrate() {
        return isMigrate;
    }
}
