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

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.ArrayList;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.AddOperation;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{
	// Needed for the logging system sgeag@2017
	private LSimWorker lsim = LSimFactory.getWorkerInstance();
	
	private Socket socket = null;
	private ServerData serverData = null;
	
	private Object mutex = new Object();
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			//Object mutex = new Object();
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] TSAE session");
			//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
			lsim.log(Level.TRACE, "-------------------------- Starting Partner Session "+current_session_number+" ------------------------------------");
			lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Expecting AE Request from a generator. Received:\n"+msg);
			if (msg.type() == MsgType.AE_REQUEST){
				MessageAErequest OriginatorAEMsg = (MessageAErequest) msg;
				lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Received Request from Originator:\n"+msg);
				
				if (serverData.getLog().listNewer(OriginatorAEMsg.getSummary()).size() > 0){
				lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Sending Operations to Originator:\n"+serverData.getLog().listNewer(OriginatorAEMsg.getSummary()));
				}
				// Send operations
				//lsim.log(Level.TRACE,  "Session "+current_session_number+": sending operations");
				for (Operation op: serverData.getLog().listNewer(OriginatorAEMsg.getSummary())){
					MessageOperation OpMsg = new MessageOperation(op);
					OpMsg.setSessionNumber(current_session_number);
					out.writeObject(OpMsg);
					//lsim.log(Level.TRACE, "Sent message: "+OpMsg);
					//lsim.log(Level.TRACE, "[TSAESessionPartnerrSide] [session: "+current_session_number+"] sent message: "+OpMsg);
				}
				
				// Send to originator: local's summary and ack				
				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;
				
				lsim.log(Level.TRACE, "Partner Session "+current_session_number+": retrieving Summary and Ack from serverData");
				// Using synchronized to make sure no other node interferes
				
				Object mutexGet = new Object();
				synchronized(mutexGet){		
					//lsim.log(Level.TRACE, "Retrieving structures from serverData: Summary, Ack");
					localSummary = serverData.getSummary().clone();
					//lsim.log(Level.TRACE, "Summary retrieved: " + localSummary);
					localAck = serverData.getAck().clone();
					//lsim.log(Level.TRACE, "Ack retrieved: " + localAck);
				}
				
				//lsim.log(Level.TRACE,  "Finished sending missing Operations to the Originator. Requesting missing Ops");
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
	 	        out.writeObject(msg);
				lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Sending structures to Originator:\n"+msg);
	 	        
				// receive operations
				msg = (Message) in.readObject();
				//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				
				
				// Store operations in a List to take care of them later
				//lsim.log(Level.TRACE, "Retrieving Operations...");
				List<MessageOperation> origin_operations = new ArrayList<MessageOperation>();
				
				while (msg.type() == MsgType.OPERATION){
					//lsim.log(Level.TRACE,  "Retrieved operation from Originator: " + msg);
					origin_operations.add((MessageOperation)msg);
					
					msg = (Message) in.readObject();
					//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				}
				
				if (origin_operations.size()>0) lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Received operations from Originator:\n"+origin_operations);
				
				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE){
					lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Received ending request from Originator");
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
		            out.writeObject(msg);					
					//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
					
		            lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Ack ending request");
		            
					List<MessageOperation> add_operations = new Vector<MessageOperation>();
	            	List<MessageOperation> remove_operations = new Vector<MessageOperation>();
	            	
	            	for (MessageOperation partnerMessageOp : origin_operations){
	            		if (partnerMessageOp.getOperation().getType() == OperationType.ADD){
	            			add_operations.add(partnerMessageOp);
	            		} else {
	            			remove_operations.add(partnerMessageOp);
	            		}
	            	}
	            	
	            	//lsim.log(Level.TRACE,  "Preparing to sent Add and Remove Ops to serverData");
	            	//lsim.log(Level.TRACE, "AddOperations: " + add_operations);
	            	//lsim.log(Level.TRACE,  "RemoveOperations: " + remove_operations);
	            	if (add_operations.size()>0) lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Sendind add_ops:\n"+add_operations);
	            	if (remove_operations.size()>0) lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Sending rm_ops:\n"+remove_operations);
	            	
	            	
		            // Using synchronized to send the AddOperations first, then RemoveOperations and, finally, update the data structures
		            Object mutexSet = new Object();
	            	synchronized(mutexSet){
		            	//lsim.log(Level.TRACE,  "Sending operations and updating in ServerData");
		            	for (MessageOperation addMessageOp : add_operations){
		            		//lsim.log(Level.TRACE, "Sending addOp: "+addMessageOp);
		            		serverData.execOperation((AddOperation)addMessageOp.getOperation());
		            	}
		            	
		            	for (MessageOperation removeMessageOp: remove_operations){
		            		//lsim.log(Level.TRACE, "Sending removeOp: "+removeMessageOp);
		            		//lsim.log(Level.FATAL, "Partner Session "+current_session_number+": Sending remove OP:\n"+removeMessageOp);
		            		serverData.execOperation((RemoveOperation)removeMessageOp.getOperation());
		            	}
		            	
		            	//lsim.log(Level.FATAL, "Passing messages from originator: "+origin_operations);
		            	
		            	/*for (MessageOperation messageOp : origin_operations){
		            		if (messageOp.getOperation().getType() == OperationType.ADD){
		            			serverData.execOperation((AddOperation) messageOp.getOperation());
		            		} else {
		            			serverData.execOperation((RemoveOperation) messageOp.getOperation());
		            		}
		            		serverData.execOperation(messageOp.getOperation());
		            		lsim.log(Level.FATAL, "Operation: "+messageOp.getOperation());;
		            		lsim.log(Level.FATAL, "Log: "+serverData.getLog());
		            	}*/
		            	
		            	lsim.log(Level.TRACE, "Partner Session "+current_session_number+": Recipes in server:\n"+serverData.getRecipes());
		            	
						/*serverData.updateSummary(originatorSummary);
						serverData.updateAck(originatorAck);
						serverData.purgeLog();*/
		            	//lsim.log(Level.TRACE, "Checking recipes from serverData: " + serverData.getRecipes().clone());
		            	//lsim.log(Level.TRACE, "Updating Summary to max...");
		            	serverData.getSummary().updateMax(OriginatorAEMsg.getSummary());
		            	lsim.log(Level.TRACE, "Partner Session "+current_session_number+": New Summary:\n"+serverData.getSummary());
		            	//lsim.log(Level.TRACE, "New summary: " + serverData.getSummary().clone());
		            	serverData.getAck().update(serverData.getId(), serverData.getSummary());
		            	//lsim.log(Level.TRACE,  "Updating Ack to max...");
		            	serverData.getAck().updateMax(OriginatorAEMsg.getAck());
		            	lsim.log(Level.TRACE, "Partner Session "+current_session_number+": New Ack (includes previous Sum):\n"+serverData.getAck());
		            	//lsim.log(Level.TRACE,  "New Ack: " + serverData.getAck().clone());
		            	//lsim.log(Level.TRACE,  "Purging Log...");
		            	serverData.getLog().purgeLog(serverData.getAck());
		            	lsim.log(Level.TRACE, "Originator Session "+current_session_number+": New Log:\n"+serverData.getLog());
		            	//lsim.log(Level.TRACE,  "New Log: " + serverData.getLog());
		            }
				}
				
			}
			socket.close();		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			//lsim.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }
		
		//lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] End TSAE session");
	}
}