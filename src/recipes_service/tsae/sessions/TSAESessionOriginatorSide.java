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
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.Operation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;

//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask{ //TimerTask es clase que programa tareas puntuales o recurrentes
	// Needed for the logging system sgeag@2017
	private LSimWorker lsim = LSimFactory.getWorkerInstance(); /* LSim es todo un tema por su cuenta. En principio asumir que es una parte del código en la que no entrar */
	private static AtomicInteger session_number = new AtomicInteger(0); // Se intenta que el número de sesión sea único
	
	private ServerData serverData;
	public TSAESessionOriginatorSide(ServerData serverData){ // Función muy sencilla para asignar datos del servidor que solicitará la sesión AE
		super(); // Para qué llama al constructor de la superclase?
		this.serverData=serverData;		
	}
	
	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){ // Ejecución de la sesión AE; detalles más abajo. ¿Quién invoca esta rutina?
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession= serverData.getRandomPartners(num); /* Lista de los servers en orden aleatorio. En clase Hosts del paquete recipes_service.communication */
		Host n;
		for(int i=0; i<partnersTSAEsession.size(); i++){  /* Ejectua la sesión AE servidor tras servidor en el orden aleatorio devuelto por la función getRandomPartners. Para los detalles de la sesión hay que ir a la función SessionTSAE */
			n=partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}
	
	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */
	private void sessionTSAE(Host n){
		int current_session_number = session_number.incrementAndGet(); // autoexplicativo
		if (n == null) return;
		
		lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] TSAE session");

		try {
			Socket socket = new Socket(n.getAddress(), n.getPort()); //Establece canal de comunicación del server partner con server que inicia la AE
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

			TimestampVector localSummary = null;
			TimestampMatrix localAck = null;
			
			// Send to partner: local's summary and ack
			/* ¿Cómo se asignan valores a las variables localSummary y localAck? La información está en serverData y las funciones en ServerData. 
			 * Habría que hacer:
			 * 
			 *  localSummary = serverData.getSummary();
			 *  localAck= serverData.getAck(); En principio el Ack estará vacío, la clase TimestampMatrix se ha de acabar de programar, 
			 *  pero no parece relevante para la Phase2: el ACK se usa con el Purge, que no hacemos aquí
			 *  
			 */
			Message	msg = new MessageAErequest(localSummary, localAck);
			msg.setSessionNumber(current_session_number);
            		out.writeObject(msg);
			lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

            // receive operations from partner
		/* ¿Cómo se 'activa' el partner para poner en marcha el run() del TSAESessionPartnerSide? No tengo ni idea, asumiré que funciona */
			msg = (Message) in.readObject();
			lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			while (msg.type() == MsgType.OPERATION){
				/* Implementar update del log con los nuevos mensajes que se van recibiendo del partner de la misma manera que en la práctica 1. 
				 * En principio algo del estilo:
				 * 
				 *  	this.log.add(op);
				 *	this.summary.updateTimestamp(timestamp); otra opción es hacer un updateMax al final de la recepción de mensajes, 
				 *      pero sería necesario un summary vacío que se llenara con los mensajes 
				 *	this.recipes.add(rcpe) o this.recipes.remove(rcpe) en función de la operación (remove todavía no se ha implementado);
				 *  
				 */
				msg = (Message) in.readObject();
				lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
			}

            // receive partner's summary and ack
			if (msg.type() == MsgType.AE_REQUEST){
				/* 
				 * Copia local de summary y ack para evitar problemas de concurrencia.
				 * TimestampVector localSummary = msg.getSummary();
				 * TimestampMatrix localAck = msg.getAck();
				 * 
				 * Ahora hay que comparar los summaries para ver qué se tiene de más que el Partner
				 * 	Conseguir lista de todos los participants
				 * 	Hacer un FOR que compare para cada Server el summary del Partner con el summary del Originator, 
				 *	comprobar si la función log.listNewer nos ahorra la faena
				 * 	Si el Summary del Partner > Summary del Originator, enviar las operaciones al Originator, construyendo el msg
				 *
				 * send operations */
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

				// send and "end of TSAE session" message
				msg = new MessageEndTSAE();  
				msg.setSessionNumber(current_session_number);
	            		out.writeObject(msg);					
				lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] sent message: "+msg);

				// receive message to inform about the ending of the TSAE session
				msg = (Message) in.readObject();
				lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
				if (msg.type() == MsgType.END_TSAE){
					// 
				}

			}			
			socket.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			lsim.log(Level.FATAL, "[TSAESessionOriginatorSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }

		
		lsim.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] End TSAE session");
	}
}
