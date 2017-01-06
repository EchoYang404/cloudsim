package org.bjut.hdfssim;

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

    public int getTotalNumOfRack() {
        Iterator<Block> blockIterator = hFile.getReplicaListById(this.id).iterator();
        Set<Integer> racks = new HashSet<>();
        while (blockIterator.hasNext()) {
            racks.add(blockIterator.next().getStorage().getDatanode().getRackId());
        }
        return racks.size();
    }
}
