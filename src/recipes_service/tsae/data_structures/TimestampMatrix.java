/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

//Import Vector to clone
import java.util.Vector;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	/**
	 * Not private for testing purposes.
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){
		return this.timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public void updateMax(TimestampMatrix tsMatrix){
		if (tsMatrix == null){
			return;
		}
		
		//Iterate through all entries in the TimestampMatrix
		for (Map.Entry<String, TimestampVector> tsMatrixElement : tsMatrix.timestampMatrix.entrySet()){
			//Get the node and TimestampVector
			String node = tsMatrixElement.getKey();
			TimestampVector tsVector = tsMatrixElement.getValue();
			
			TimestampVector tsNode = this.getTimestampVector(node);
			
			//If the node exists in the local TimestampMatrix, update its TimestampVector with the max value
			if(tsNode != null){

				tsNode.updateMax(tsVector);
			}
			
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public void update(String node, TimestampVector tsVector){
		if (node != null && tsVector != null){

		if (this.timestampMatrix.containsKey(node)){
		this.timestampMatrix.replace(node,  tsVector);
		} else {
			this.timestampMatrix.put(node, tsVector);}
		}
}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public TimestampVector minTimestampVector(){
		//Create an empty TimestampVector
		TimestampVector minTsV = null;
		
		//Retrieve the list of participant nodes from local Matrix
		List<String> participants = new Vector<String>(timestampMatrix.keySet());
		
		//For every participant, retrieve its TimestampVector. If the minimum Vector is null, it gets a copy.
		//If not, it merges with the minimum value.
		for (String node : participants){
			TimestampVector tsVector = getTimestampVector(node);
			if (minTsV == null){
				minTsV = tsVector.clone();
			} else {
				minTsV.mergeMin(tsVector);
			}
		}
		return minTsV;
	}

	
	/**
	 * clone
	 */
	public TimestampMatrix clone(){
		// Create a list of participants extracting it from this TimestampMatrix
		List<String> participants = new Vector<String>(timestampMatrix.keySet());
		
		//Call the TimestampMatrix constructor
		TimestampMatrix clonedTsMatrix = new TimestampMatrix(participants);
		
		
		for (String node : participants){
			clonedTsMatrix.timestampMatrix.put(node, this.getTimestampVector(node).clone());
			//clonedTsMatrix.update(node, getTimestampVector(node).clone());
		}
		return clonedTsMatrix;
	}
	
	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj){
			return true;
		} else if (obj == null){
			return false;			
		} else if (getClass() != obj.getClass()){
			return false;
		}
		
		TimestampMatrix other = (TimestampMatrix) obj;
		
		return timestampMatrix.equals(other.timestampMatrix);
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}