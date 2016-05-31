/**
 * Socket Server manages network connections 

 * 
 * @author Mosharaf Chowdhury (http://www.mosharaf.com)
 * @author Prashanth Mohan (http://www.cs.berkeley.edu/~prmohan)
 * 
 * Copyright (c) 2012, University of California at Berkeley
 * All rights reserved.
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *  * Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of University of California, Berkeley nor the
 *    names of its contributors may be used to endorse or promote products
 *    derived from this software without specific prior written permission.
 *    
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 *  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 *  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 *  DISCLAIMED. IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY
 *  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 *  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 *  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 *  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 *  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */


package edu.berkeley.cs162;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

/** 
 * This is an generic class that should handle all TCP network connections 
 * arriving on a given unique (host, port) tuple. Ensure that this class 
 * remains generic by providing the connection handling logic in a NetworkHandler
 */

public class SocketServer {
	String hostname;
	int port;
	NetworkHandler handler;
	ServerSocket server;
	boolean isClose;
	//+chen
	private static final int timeout = 20000;
	//-chen
	
	public SocketServer(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}
	
	public void connect()  throws IOException {
	      // TODO: implement me
		/*
		try {
			server = new ServerSocket( port);
		} catch (IOException e) {
			//....................
		} */
		while (server == null) {
			try {
				System.out.println("123456789............" + port);
				//isOpen = true;
			//	server = new ServerSocket(port);
				server = new ServerSocket(port);
		//		server.bind(new InetSocketAddress("", 0));
				
			//	System.out.println("newed socket");
				server.setSoTimeout(timeout);
				port = server.getLocalPort(); //Could be deleted!
	
				System.out.println("CONNECT " + server.getInetAddress() + "@@@" + port);
	/*			if (server != null) {
					break;
				} */
			} catch (IOException e) { }
		}		
	}
	
	/**
	 * Accept requests and service them asynchronously. 
	 * @throws IOException if there is a network error (for instance if the socket is inadvertently closed) 
	 */
	public void run() throws IOException {
	      // TODO: implement me
		Socket socket = null;
		while (!isClose) {
			try {
			    //System.out.println("hhhhhhhhhhhhhhhhh");
				socket = server.accept();
				System.out.println("AAAAAAAAAAAA server accept: " + socket.getInetAddress() + "@ " + socket.getPort());
				if (handler != null) {
					handler.handle(socket);
				}
			} catch (IOException e) {
				//System.out.println("jjjj");
			//	System.out.println(e.getMessage());
			//	connect();
			}
		}
		
		try {
			server.close();
		} catch (IOException e) {}
	}
	
	/** 
	 * Add the network handler for the current socket server
	 * @param handler is logic for servicing a network connection
	 */
	public void addHandler(NetworkHandler handler) {
		this.handler = handler;
	}

	/**
	 * Stop the ServerSocket
	 */
	public void stop() {
	      // TODO: implement me
		finalize();
	}
	
	private void closeSocket() {
	     // TODO: implement me
		isClose = true;
		try {
			server.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected void finalize(){

		closeSocket();
	}
}


