/**
 * Client component for generating load for the KeyValue store. 
 * This is also used by the Master server to reach the slave nodes.
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
import java.net.Socket;


/**
 * This class is used to communicate with (appropriately marshalling and unmarshalling) 
 * objects implementing the {@link KeyValueInterface}.
 *
 * @param <K> Java Generic type for the Key
 * @param <V> Java Generic type for the Value
 */
public class KVClient implements KeyValueInterface {

	private String server = null;
	private int port = 0;
	
	/**
	 * @param server is the DNS reference to the Key-Value server
	 * @param port is the port on which the Key-Value server is listening
	 */
	public KVClient(String server, int port) {//"localhost", 8080
		this.server = server;
		this.port = port;
	}
	
	//+chen
	public String getServer() {
		return server;
	}
	
	public int getPort() {
		return port;
	}
	//-chen
	
	private Socket connectHost() throws KVException {
	    // TODO: Implement Me!  
		//return null;
		Socket sock = null;
		try {
			sock = new Socket(this.server, this.port);
			sock.setSoTimeout(5000);
		} catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Network Error: Could not create socket"));
		}
		return sock;
	}
	
	private void closeHost(Socket sock) throws KVException {
	    // TODO: Implement Me!
		//// Close socket and catch IOException and wrap them in a KVException
		try {
			sock.close();
		} catch (IOException e) {
			throw new KVException(new KVMessage("resp", e.getMessage()));
		}
	}
	
	public boolean put(String key, String value) throws KVException {
	    // TODO: Implement Me!
	    //return true;
		KVMessage toSend = new KVMessage("putreq");
		Socket sock = connectHost();
		toSend.setKey(key);
		toSend.setValue(value);
		toSend.sendMessage(sock);
		
		try {
			KVMessage toReceive = new KVMessage(sock.getInputStream());
			//System.out.println("PUT PUTPUTPUT " + toReceive.getMsgType() +" " + toReceive.getMessage() );
			if (!toReceive.getMsgType().equals("resp") || !toReceive.getMessage().equals("Success")) {
				//System.out.println("PUT PUTPUTPUT " + toReceive.getMsgType() +toReceive.getMessage() );
				throw new KVException(toReceive);
			}
		} catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: put failed " + e.getMessage()));			
		}
		
		return true;
	}


	public String get(String key) throws KVException {
	    // TODO: Implement Me!
	    // return null;
		KVMessage request = new KVMessage("getreq");
		Socket sock = connectHost();
		request.setKey(key);
		request.sendMessage(sock);
		String value = null;
		try {
			KVMessage response = new KVMessage(sock.getInputStream());
			if (!response.getMsgType().equals("resp") || !response.getMessage().equals("Success")) {
				System.out.println("GET GETGETGET " + response.getMsgType() +response.getMessage() );
				System.err.println(response.getMessage());
				throw new KVException(response);
			}
			value = response.getValue();
			if (value == null || value.length() == 0) {
				System.out.println(response.getMessage());
				//throw new KVException(response);
				return null;
			}
			
		} catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: get failed " + e.getMessage()));			
		}
		return value;
	}
	
	public void del(String key) throws KVException {
	    // TODO: Implement Me!
		KVMessage request = new KVMessage("delreq");
		request.setKey(key);
		Socket sock = connectHost();
		request.sendMessage(sock);
		
		try {
			KVMessage response = new KVMessage(sock.getInputStream());
			if (!response.getMsgType().equals("resp") || !response.getMessage().equals("Success")) {
				throw new KVException(response);
			}
		} catch (IOException e) {
			throw new KVException(new KVMessage("resp", "Unknown Error: delete failed " + e.getMessage()));		
		}
	}	
}
