package edu.berkeley.cs162;

import static edu.berkeley.cs162.StaticConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {
    private TPCMaster tpcMaster;
    private ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
        // implement me
    	this.tpcMaster = tpcMaster;
    	this.threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
    	Runnable clientHandler = new ClientHandler(client);
    	try {
    		threadPool.addToQueue(clientHandler);
    	} catch (InterruptedException e) {}  	
    }

    /**
     * Runnable class containing routine to service a request from the client.
     */
    private class ClientHandler implements Runnable {
        private Socket client = null;
        /**
         * Construct a ClientHandler.
         *
         * @param client Socket connected to client with the request
         */
        public ClientHandler(Socket client) {
            this.client = client;
        }

        /**
         * Processes request from client and sends back a response with the
         * result. The delivery of the response is best-effort. If we are
         * unable to return any response, there is nothing else we can do.
         */
        @Override
        public void run() {
            // implement me
        	KVMessage request = null;
        	KVMessage response = null;
        	try {
        		request = new KVMessage(client);
        		response = new KVMessage(RESP);
        		String msgType = request.getMsgType();
        		if (msgType.equals(GET_REQ)) {
        			String value = tpcMaster.handleGet(request);
            		response.setKey(request.getKey());
            		response.setValue(value);
        		} else if (msgType.equals(PUT_REQ)) {
        			tpcMaster.handleTPCRequest(request, true);
        			response.setMessage(SUCCESS);
        		} else if (msgType.equals(DEL_REQ)) {
        			tpcMaster.handleTPCRequest(request, false);
        			response.setMessage(SUCCESS);
        		}
        		response.sendMessage(client);    		
        	} catch (KVException e) {
        		response = e.getMsg();
        		try {
					response.sendMessage(client);
				} catch (KVException e1) {
				}
        	} finally {
        		try {
					client.close();
				} catch (IOException e) {}
        	}
        	
        }
    }

}
