package edu.berkeley.cs162;

import static edu.berkeley.cs162.StaticConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    private long slaveID;
    private KVServer kvServer;
    private TPCLog tpcLog;
    private ThreadPool threadpool;
    

    KVMessage phase1Msg = null;      

    private static final int MAX_KEY_SIZE = 256;
    private static final int MAX_VAL_SIZE = 256 * 1024;
    
    private void validateKey(String key) throws KVException {
    	if (key == null || key.length() == 0) {
    		throw new KVException("Key is empty");
    	} else if (key.length() > MAX_KEY_SIZE) {
    		throw new KVException("Key oversized");
    	}
    }
    
    private void validateValue(String value) throws KVException {
    	if (value == null || value.length() == 0) {
    		throw new KVException("Value is empty");
    	} else if (value.length() >  MAX_VAL_SIZE) {
    		throw new KVException("Value oversized");
    	}
    }


    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 10);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
        // implement me
    	Socket master = null;	
    	try {
        	master = new Socket(masterHostname, 9090);
        	//String regiSlaveInfo = slaveID + "@" + server.hostname + ":" + server.port;//chen Attention here!!!
        	String regiSlaveInfo = slaveID + "@" + server.hostname + ":" + master.getPort();
        	//System.out.println("regi info : "+ regiSlaveInfo);
        	
        	KVMessage regi = new KVMessage(REGISTER, regiSlaveInfo);

        	regi.sendMessage(master);

        	KVMessage response = new KVMessage(master);
        	String expectedMsg = "Successfully registered " + regiSlaveInfo;
        
        	if (!response.getMsgType().equals(RESP) || !response.getMessage().equals(expectedMsg)) {
        		System.err.println("response of register KVMessage is not as expected");
        			throw new KVException(ERROR_INVALID_FORMAT);       		
        	}
    	} catch (IOException e) {
    	
    		throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
    	} finally {
    		try {
				master.close();
			} catch (IOException e) {}
    	}

    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        // implement me
    	Runnable masterHandler = new MasterHandler(master);
    	try {
			threadpool.addToQueue(masterHandler);
		} catch (InterruptedException e) {}
    }

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable{
        private Socket master;
        /**
         * Construct a MasterHandler.
         * @param master Socket connected to master with the message
         */
        public MasterHandler(Socket master) {
            this.master = master;
        }

        /**
         * Processes request from master and sends back a response with the
         * result. This method needs to handle both phase1 and phase2 messages
         * from the master. The delivery of the response is best-effort. If
         * we are unable to return any response, there is nothing else we can do.
         */
        
        @Override
        public void run() {
            // implement me
        	KVMessage masterMsg = null;
        	KVMessage response = null;
        	try {
        		masterMsg = new KVMessage(master);
        		
        		String masterMsgType = masterMsg.getMsgType();
        	    String key = null;
        	    String value = null;
        	    System.out.println("@@@@TPCMasterHandler msg type: " + masterMsgType);
        		
        		switch(masterMsgType) {
        		//PHASE 1: prepare message from master
        		case GET_REQ:
        			try {
        				value = kvServer.get(masterMsg.getKey());//Might throw out KVException
        				response = new KVMessage(RESP);
        				response.setKey(masterMsg.getKey());
        				response.setValue(value);	
        				
        			//	phase1Msg = masterMsg;//?????????????????Attention here!!!
        			//	System.out.println("@GET_req :  phase1Msg is : " + phase1Msg.toString());
        			} catch (KVException e) {//throw out by kvServer.get(...);
        				System.out.println("@TPCMasterHandler run() : KVException, the key does not exist::::::::::: " + e.getMsg().toXML());
        				String errorInfo = e.getMsg().getMessage();
        				response = new KVMessage(RESP, errorInfo);
        			}
        			break;
        			
        		case PUT_REQ:
        			key = masterMsg.getKey();
        			value = masterMsg.getValue();
        			try {
        				validateKey(key);
        				validateValue(value);
        				response = new KVMessage(READY);        				
        				phase1Msg = masterMsg;//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1      
        				
        				System.out.println("@put_req :  phase1Msg is : " + phase1Msg);
        			} catch (KVException e) {
        				String errorInfo = e.getMsg().getMessage();
        				response = new KVMessage(ABORT_VOTE, errorInfo);
        			}  
        			tpcLog.appendAndFlush(masterMsg);
        			break;
        			
        		case DEL_REQ:
        			key = masterMsg.getKey();
        			try {
        				validateKey(key);
        				//System.out.println("BEF kvServer.get()");
        				kvServer.get(key);
        				//System.out.println("AFTER kvServer.get()");
        				response = new KVMessage(READY);
        				
        				phase1Msg = masterMsg;//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
        				System.out.println("@put_req :  phase1Msg is : " + phase1Msg);
        			} catch (KVException e) {
        				String errorInfo = e.getMsg().getMessage();
        				System.out.println("MASTER HANDLER : PHASE1, DEL_REQ: KVEXCEPTION errorInfo : " + errorInfo);

        				response = new KVMessage(ABORT_VOTE, errorInfo);
        			}
        			tpcLog.appendAndFlush(masterMsg);
        			break;
        			
        		//PHASE 2: decision message from master	
        		case ABORT_DECISION:
        			System.out.println("@MasterHandler: RECEIVED ABORT_DECISION");
        			response = new KVMessage(ACK);
        			phase1Msg = null;//Attention here?????????????? Correct or not????/
        			tpcLog.appendAndFlush(masterMsg);
        			break;
        		case COMMIT:       			
        			KVMessage requestMsg = tpcLog.getLastEntry(); 	
        			System.out.println("ATTENTION!! : requestMsg: " + requestMsg.getMsgType());
        			
        			System.out.println("@MasterHandler: RECEIVED COMMIT");
        			
        			String msgType = null;
        			if (phase1Msg == null && allDoneBeforeCorrupt()) {
        				 response = new KVMessage(ACK);
        			} else {				
        				msgType = phase1Msg == null ?  tpcLog.getLastEntry().getMsgType() : phase1Msg.getMsgType();			
        			    response = new KVMessage(ACK);//Attention here!!!! No matter put and del failed or succeed, slave will always send an ACK back;

        			switch (msgType) {
        			case PUT_REQ:        				
        				key = phase1Msg.getKey();
        				value = phase1Msg.getValue();
        				try {
        					kvServer.put(key, value);
        				} catch (KVException e) {//Do nothing here................   				
        				}
        				if (phase1Msg != null) {
        					tpcLog.appendAndFlush(masterMsg);
        				}
        				break;
        			case DEL_REQ:
        				key = phase1Msg.getKey();
        				try {
        					kvServer.del(key);
        				} catch (KVException e) {
        					e.printStackTrace();
        				}
        				if (phase1Msg != null) {
        					tpcLog.appendAndFlush(masterMsg);
        				}
        				break;		
        			default: 
        				  //Have nothing to do 					
        					break;			
        			}     	
        			phase1Msg = null;  
        			}

        			break; //COMMIT END 			   			
        			default :
        				 throw new KVException(new KVMessage(RESP, "Message format from master is incorrect"));
        		}
        		
        		//Attention here!!!!      		
        		response.sendMessage(master);
        	} catch (KVException e) {
        		KVMessage errorMsg = e.getMsg();
        		try {
        			errorMsg.sendMessage(master);
        		} catch (KVException kvException) {} 
        	}
        }

    }
    
    private boolean allDoneBeforeCorrupt() {
    	KVMessage requestMsg = tpcLog.getLastEntry(); 	
		String msgType =  requestMsg.getMsgType();
		if (msgType.equals("COMMIT") || msgType.equals("ABORT_DECISION")) {
				return true;
		} 
		return false;
    }

}
