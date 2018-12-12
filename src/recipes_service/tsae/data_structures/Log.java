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
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias December 2012
 *
 */
public class Log implements Serializable {
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations received by a
	 * client. They are stored in a ConcurrentHashMap (a hash table), that
	 * stores a list of operations for each member of the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log = new ConcurrentHashMap<String, List<Operation>>();

	public Log(List<String> participants) {
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext();) {
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are inserted in order. If
	 * the last operation for the user is not the previous operation than the
	 * one being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public synchronized boolean add(Operation op) {
		//lsim.log((Level.TRACE, "Inserting into Log the operation: " + op);

		// Get the HostId from the operation to insert
		Timestamp opTimestamp = op.getTimestamp();
		String opHostId = opTimestamp.getHostid();

		// Retrieve the list of Operations that are stored in the log for that
		// HostId
		List<Operation> opHostIdOperations = log.get(opHostId);

		// Retrieve the last timestamp in the list of Operations
		Timestamp lastTimestampHostId;

		if (opHostIdOperations == null) {
			lastTimestampHostId = null;
		} else if (opHostIdOperations.isEmpty()) {
			lastTimestampHostId = null;
		} else {
			lsim.log(Level.TRACE, "This list should be ordered!"+opHostIdOperations);
			lastTimestampHostId = opHostIdOperations.get(opHostIdOperations.size() - 1).getTimestamp();
		}

		// Compare both timestamps. The new incoming operation timestamp must be the next in sequence

		long tsDiff = opTimestamp.compare(lastTimestampHostId);

		if (tsDiff == 1 || (lastTimestampHostId == null && tsDiff == 0)) {
		//if ((lastTimestampHostId != null && tsDiff == 1 || lastTimestampHostId == null && tsDiff == 0)) {
			//lsim.log(Level.DEBUG, "Inserting operation: " + op);
			log.get(opHostId).add(op);
			return true;
		} else {
			//lsim.log(Level.ERROR, "Insertion of operations " + op + " failed.");
			return false;
		}
	}

	/**
	 * Checks the received summary (sum) and determines the operations contained
	 * in the log that have not been seen by the proprietary of the summary.
	 * Returns them in an ordered list.
	 * 
	 * @param sum
	 * @return list of operations
	 */
	public synchronized List<Operation> listNewer(TimestampVector sum) {

		// Create an empty list using the Vector class that is already imported
		List<Operation> missingOps = new Vector<Operation>();

		// Iterate through the operations in the log. For each one, its
		// timestamp is compared with the one in the summary
		// If the stored operation is newer, it is added to the list of missing
		// operations

		for (String node : log.keySet()) {
			Timestamp summaryTs = sum.getLast(node);
			List<Operation> operations = log.get(node);

			for (Operation op : operations) {
				if (op.getTimestamp().compare(summaryTs) > 0) {
					missingOps.add(op);
				}
			}
		}

		return missingOps;
	}

	/**
	 * Removes from the log the operations that have been acknowledged by all
	 * the members of the group, according to the provided ackSummary.
	 * 
	 * @param ack: ackSummary.
	 */
	public synchronized void purgeLog(TimestampMatrix ack) {
		if (ack == null){
			return;
		}
		
		//Get the minimum Vector from the received ack
		TimestampVector minVector = ack.minTimestampVector();
		
		if (minVector != null){
			//Iterate through all entries in the local Log
			for (Map.Entry<String, List<Operation>> logOperations: log.entrySet()){
				//Retrieve the node from the operation
				String participant = logOperations.getKey();
				//Retrieve the minimum Timestamp for the node
				Timestamp minTs = minVector.getLast(participant);
				//If there is no Timestamp, the iteration continues
				if (minTs == null){
					continue;
				}
				
				//Retrieve the list of operations for the node
				List<Operation> operations = logOperations.getValue();
				
				
				//Iterate through the operations in backwards because the length of the list will change
				//as we remove operations
				int max_index = operations.size()-1;
				
				for (int i = max_index; i >= 0; i--){
					Operation op = operations.get(i);
					long tsDiff = op.getTimestamp().compare(minTs);
					
					if (tsDiff <= 0){
						operations.remove(i);
					}
					
					
				}
			
			}
		}
		
	}

	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		// Check if obj is this same instance, if it's null or if shares the
		// same class
		if (this == obj) {
			return true;
		} else if (obj == null) {
			return false;
		} else if (getClass() != obj.getClass()) {
			return false;
		}

		// Make a copy of obj, typecasting it as a Log and we use that to
		// compare using the equals method
		// of ConcurrentHashMap

		Log other = (Log) obj;

		return log.equals(other.log);

	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name = "";
		for (Enumeration<List<Operation>> en = log.elements(); en.hasMoreElements();) {
			List<Operation> sublog = en.nextElement();
			for (ListIterator<Operation> en2 = sublog.listIterator(); en2.hasNext();) {
				name += en2.next().toString() + "\n";
			}
		}

		return name;
	}
}