package edu.berkeley.cs162;

import static edu.berkeley.cs162.StaticConstants.*;

import java.io.IOException;
import java.net.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.*;

/**
 * Data structure to maintain information about SlaveServers
 */
public class TPCSlaveInfo {
	
    private long slaveID;
    private String hostname;
    private int port;
//+chen
    private static final Pattern SLAVE_INFO_REGEX = Pattern.compile("^(.+)@(.+):(.+)$");
    
    private WriteLock writelock;
    public WriteLock getWriteLock() {
    	return writelock;
    }
    
    //-chen
    /**
     * Construct a TPCSlaveInfo to represent a slave server.
     *
     * @param info as "SlaveServerID@Hostname:Port"
     * @throws KVException ERROR_INVALID_FORMAT if info string is invalid
     */
    public TPCSlaveInfo(String info) throws KVException {//info is: slaveID@ss.hostname:ss.port
        // implement me
    	final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    	writelock = lock.writeLock();
    	Matcher slaveInfo = SLAVE_INFO_REGEX.matcher(info);
    	if (!slaveInfo.matches()) {
    		throw new KVException(ERROR_INVALID_FORMAT);//???????Or change to "Registration error: Received slave information is unparsable";
    	}
    	slaveID = Long.parseLong(slaveInfo.group(1)); 
    	hostname = slaveInfo.group(2);
    	port = Integer.parseInt(slaveInfo.group(3));
    	if (port < 0 || port > 65535) {
    		throw new KVException("Registration error: port number is not valid");
    	}
    }

    public long getSlaveID() {
        return slaveID;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    /**
     * Create and connect a socket within a certain timeout.
     *
     * @return Socket object connected to SlaveServer, with timeout set
     * @throws KVException ERROR_SOCKET_TIMEOUT, ERROR_COULD_NOT_CREATE_SOCKET,
     *         or ERROR_COULD_NOT_CONNECT
     */
    public Socket connectHost(int timeout) throws KVException {
        // implement me
        //return null;
    	Socket sock;
    	try {
    		//System.out.println("############$$$$$$$$$$$$$$###########" + port);
    		//sock = new Socket(hostname, port);
    		sock = new Socket();
    		//System.out.println("########################slave INFO TRY TO CONNECT : " + hostname + "@ " + port);
			//System.out.println("%%%%%%%%%%%%%%%%%new sock port: " + sock.getLocalPort());
    		sock.setSoTimeout(timeout);
			sock.connect(new InetSocketAddress(hostname, port), timeout);
			//System.out.println("########################" + port);
			return sock;
		} catch (UnknownHostException e) {
			throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
		} catch (SocketException e1) {
			System.err.println(e1.getMessage());
			throw new KVException(ERROR_SOCKET_TIMEOUT);
		} catch (IOException e2) {
			throw new KVException(ERROR_COULD_NOT_CONNECT);
		}
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        // implement me
    	try {
			sock.close();
		} catch (IOException e) {}
    	
    }
    
    //+chen ??????????????????????????????????????????????????/
    public synchronized void update(String hostname, int port) {
    	this.hostname = hostname;
    	this.port = port;
    }
       
    public synchronized KVMessage excecuteKVMsg(KVMessage msg) throws KVException{
    	Socket sock = null;
    	KVMessage response = null;
    	try {
			sock = connectHost(TIME_OUT);
			System.out.println("slave info connect: " + sock.getInetAddress() + "@@@@@@@" + sock.getLocalPort());
			msg.sendMessage(sock);
			response = new KVMessage(sock);
			sock.close();
			return response;
		} 
  /*  	catch (KVException e) {//throw out directly 
			//??????????????????????????		
			 response = e.getMsg();
			 System.out.println("@SLAVEINFO : " + response.toXML());
		} */  
    	catch (IOException e) {
			
			throw new KVException(new KVMessage(RESP, e.getMessage()));
		} finally {			
			if (sock != null) {
				try {
					sock.close();
				} catch (IOException e) {}
			}
			return response;
		}
    }
    
    public KVMessage TPCDeleteVote(String key) throws KVException {
			KVMessage msg = new KVMessage(DEL_REQ);
			msg.setKey(key);
			KVMessage response =  null;
	//		System.out.println("@SLAVEINFO: TPCDeleteVote: after executeKVMsg...SHOULD REVEICED AN RESPONSE:::" + response.toXML());
			
			try {
				response = excecuteKVMsg(msg);//if socket error, throw out socket excetion
			} catch (KVException e) {
				response = new KVMessage(ABORT_VOTE, e.getMsg().getMessage());
			}
			
			if (response.getMsgType().equals(ABORT_VOTE) || response.getMsgType().equals(READY)) {
				return response;
			} else {//the response msg type was only suppose to be ABORT_VOTE or READY
				throw new KVException(INVALID_RESPONSE);
			}
    }
    
    public KVMessage TPCPutVote(String key, String value) throws KVException {    
    		KVMessage msg = new KVMessage(PUT_REQ);
    		msg.setKey(key);
    		msg.setValue(value);
    		System.out.println("((((((((((((((((((((((((((((((((((((((((((((((((");
   
			KVMessage response =  null;

			try {
				response = excecuteKVMsg(msg);//if socket error, throw out socket excetion
			} catch (KVException e) {
				response = new KVMessage(ABORT_VOTE, e.getMsg().getMessage());
			}
		
			if (response.getMsgType().equals(ABORT_VOTE) || response.getMsgType().equals(READY)) {
				return response;
			} else {//the response msg type was only suppose to be ABORT_VOTE or READY
				throw new KVException(INVALID_RESPONSE);
			}
    }
    /*
    public String get(String key) {
    	try {
    		KVMessage msg = new KVMessage(GET_REQ);
        	msg.setKey(key);
        	KVMessage resp = excecuteKVMsg(msg); 
        	if (!resp.getKey().equals(key)) {
        		throw new KVException(INVALID_RESPONSE);//Invalid key from slave
        	}
        	if (!resp.getMsgType().equals(RESP) || resp.getValue() == null) {
        		throw new KVException(resp);
        	}
        	return resp.getValue();
    	} catch (KVException e) {
    		return null;
    	}
    } */
    
    public String get(String key) throws KVException{
    		KVMessage msg = new KVMessage(GET_REQ);
        	msg.setKey(key);
        	KVMessage resp = excecuteKVMsg(msg); //Might throw out KVException!!! Say, TIMEOUT
        	
        	if (resp.getMessage() != null) {
        		System.out.println("88888888888888888888888888");
        		throw new KVException(resp);
        	}
       
        	if (!resp.getMsgType().equals(RESP) || resp.getValue() == null) {
        		System.out.println("@TPCSlaveInfo ... get() get value == null, throw KVException...");
        		throw new KVException(resp);
        	}
        	if (!resp.getKey().equals(key)) {
        		throw new KVException(INVALID_RESPONSE);//Invalid key from slave
        	}
        	System.out.println("get @ slaveInfo response KVMessage: " + resp.toXML());
        	return resp.getValue();
    }
    
    public void TPCCommit() {
    	boolean finished = false;
    	while (!finished) {
    		try {
    			KVMessage commitMsg = new KVMessage(COMMIT);
    			KVMessage response = excecuteKVMsg(commitMsg);
    			if (response.getMsgType().equals(ACK)) {
    				finished = true;
    			} else {//Just for the notice....
    				System.out.println("Commit response is not ACK");
    			}
    		} catch (KVException e) {
    		}
    	}
    }
   
    public void TPCAbort() {
    	boolean finished = false;
    	while (!finished) {
    		try {
    			//KVMessage abortMsg = new KVMessage(ABORT_VOTE);
    			KVMessage abortMsg = new KVMessage(ABORT_DECISION);
    			KVMessage response = excecuteKVMsg(abortMsg);
    			if (response.getMsgType().equals(ACK)) {
    				finished = true;
    			} else {
    				System.out.println("Response for abort note is not ACK");
    			}
    		} catch (KVException e) {
    			//........................
    		}
    	}
    }
    //-chen
}
