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

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
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
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] TSAE session");
			lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
			if (msg.type() == MsgType.AE_REQUEST){
				/* 
				 * Copia local de summary y ack para evitar problemas de concurrencia
				 * TimestampVector localSummary = msg.getSummary();
				 * TimestampMatrix localAck = msg.getAck();
				 * 
				 * Ahora hay que comparar los summaries para ver qué se tiene de más que el Originator
				 * 	Conseguir lista de todos los participants
				 * 	Hacer un FOR que compare para cada Server el summary del Originator con el summary del Partner, comprobar si 
				 *	la función log.listNewer nos ahorra la faena
				 * 	Si el Summary del Partner > Summary del Originator, enviar las operaciones al Originator, construyendo el msg
				 *
				 * send operations */
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

		
				// send to originator: local's summary and ack
				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;
				// PBG
				/* TimestampVector localSummary = null;
				TimestampMatrix localAck = null; */
				// PBG
				/* TimestampVector localSummary = serverData.getSummary();
				TimestampMatrix localAck = serverData.getAck(); */
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
	 	        	out.writeObject(msg);
				lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

	            // receive operations
				msg = (Message) in.readObject();
				lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				while (msg.type() == MsgType.OPERATION){
					/* Se ha de implementar función type() y no sé si se tiene que cambiar OPERATION por otro tag o ya va bien ese */
					/* Implementar update del log con los nuevos mensajes que se van recibiendo del Originator de la misma manera que 
					 * en la práctica 1. En principio algo del estilo:
					 * 
					 *  	this.log.add(op);
					 *	this.summary.updateTimestamp(timestamp);
					 *	this.recipes.add(rcpe) o this.recipes.remove(rcpe) en función de la operación;
					 *  
					 */
					msg = (Message) in.readObject();
					lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				}
				
				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
		            		out.writeObject(msg);					
					lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
				}
				/* Estaría bien imprimir por pantalla los summaries de Originator y Partner para comprobar que se han sincronizado */
			}
			socket.close();		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			lsim.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }
		
		lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] End TSAE session");
	}
}
