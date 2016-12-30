/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.bjut.hdfssim;

import java.util.ArrayList;
import java.util.List;

public abstract class Storage {
	private int id;
	private double capacity;
	private double usedSize;
	private List<Block> blockList;
	private double maxTransferRate;

	public Storage(int id, double capacity)
	{
		this.id = id;
		this.capacity = capacity;
		this.blockList = new ArrayList<>();
	}
	/**
	 * Gets the id of the storage.
	 * 
	 * @return the id of this storage
	 */
	public int getId() {
		return id;
	}

	/**
	 * Gets the total capacity of the storage in MByte.
	 * 
	 * @return the capacity of the storage in MB
	 */
	public double getCapacity() {
		return capacity;
	}

	/**
	 * Gets the current size of the storage in MByte.
	 * 
	 * @return the current size of the storage in MB
	 */
	public double getCurrentSize() {
		return usedSize;
	}

	/**
	 * Gets the maximum transfer rate of the storage in MByte/sec.
	 * 
	 * @return the maximum transfer rate in MB/sec
	 */
	public double getMaxTransferRate() {
		return this.maxTransferRate;
	}

	/**
	 * Gets the available space on this storage in MByte.
	 * 
	 * @return the available space in MB
	 */
	public double getAvailableSpace() {
		return capacity - usedSize;
	}

	/**
	 * Sets the maximum transfer rate of this storage system in MByte/sec.
	 * 
	 * @param rate the maximum transfer rate in MB/sec
	 * @return <tt>true</tt> if the setting succeeded, <tt>false</tt> otherwise
	 */
	public boolean setMaxTransferRate(int rate) {
		this.maxTransferRate = rate;
		return true;
	}

	/**
	 * Checks if the storage is full or not.
	 * 
	 * @return <tt>true</tt> if the storage is full, <tt>false</tt> otherwise
	 */
	public boolean isFull() {
		if(capacity - usedSize > Configuration.getIntProperty("blockSize"))
		{
			return true;
		}
		return false;
	}

	/**
	 * Gets the number of files stored on this device.
	 * 
	 * @return the number of stored files
	 */
	public int getNumStoredBlock() {
		return blockList.size();
	}

	public boolean addBlock(Block block)
	{
		if(block.addStorage(this))
		{
			this.blockList.add(block);
			this.usedSize += block.getSize();
			return true;
		}
		return false;
	}

	public boolean deleteBlock(Block block)
	{
		block.deleteStorage(this);
		blockList.remove(block);
		this.usedSize -= block.getSize();
		return true;
	}
}
