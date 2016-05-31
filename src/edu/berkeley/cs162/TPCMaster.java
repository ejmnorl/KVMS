package edu.berkeley.cs162;

import static edu.berkeley.cs162.StaticConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

public class TPCMaster {
    private int numSlaves;
    private KVCache masterCache;

    public static final int TIMEOUT = 3000;
    public TreeMap<Long,TPCSlaveInfo> slaves;
    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        slaves = new TreeMap<Long, TPCSlaveInfo>(new UnsignedLongComparator());
    }
    
    //+chen
    class UnsignedLongComparator implements Comparator<Long>
    {
        public int compare(Long n1, Long n2)
        {
            if(isLessThanUnsigned(n1, n2)) return -1;
            else if(isLessThanUnsigned(n2, n1)) return 1;
            else return  0;
        }
    }
    
    
    //-chen
    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered.Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        // implement me
    	Long slaveID = new Long(slave.getSlaveID());
    	synchronized(slaves) {
    		if (slaves.containsKey(slaveID)) {
    			slaves.get(slaveID).update(slave.getHostname(), slave.getPort());
    		} else {
    			slaves.put(slaveID,  slave);
    		}
    	}
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
        // implement me
       // return null;
    	Long slaveID = new Long(hashTo64bit(key));
    	if (slaves.ceilingEntry(slaveID) == null) {
    		return slaves.firstEntry().getValue();
    	}
    	return slaves.ceilingEntry(slaveID).getValue();
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
    	Long slaveID = new Long(firstReplica.getSlaveID());
    	if (slaves.higherEntry(slaveID) == null) {
    		return slaves.firstEntry().getValue();
    	}
    	return slaves.higherEntry(slaveID).getValue();
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    	
      public void handleTPCRequest(KVMessage msg, boolean isPutReq)//+chen removed synchronized keyword
    	            throws KVException {//PUT OR DELETE	
        // implement me    	
    	String key = msg.getKey();
    	String value = msg.getValue();
    	WriteLock masterCacheLock = masterCache.getWriteLock(key);
    	
    	masterCacheLock.lock();
    	
    	TPCSlaveInfo slave1 = findFirstReplica(key);
    	TPCSlaveInfo slave2 = findSuccessor(slave1);
    	
    	SlaveVoteRunnable sv1 = new SlaveVoteRunnable(slave1, key, value, isPutReq);
    	SlaveVoteRunnable sv2 = new SlaveVoteRunnable(slave2, key, value, isPutReq);
    	
    	Thread s1Thread = new Thread(sv1);
    	Thread s2Thread = new Thread(sv2);
    	
    	s1Thread.start();
    	s2Thread.start();
    	
    	try {
    		s1Thread.join();
    		s2Thread.join();
    	} catch (InterruptedException e) {
    		masterCacheLock.unlock();
    		throw new KVException("Internal: TPCRequest Failed @ PHASE1: Interrupted exception");
    	}
    	
    	KVMessage s1VoteFailed = sv1.voteFailed;
    	KVMessage s2VoteFailed = sv2.voteFailed;
    	
    	KVMessage errorMsgFromPhase1 = null;
    	
    	boolean isCommit = false;
    	if (s1VoteFailed == null && s2VoteFailed == null && !sv1.isAbort && !sv2.isAbort) {
    		isCommit = true;
    	} else if (s1VoteFailed == null && s2VoteFailed != null) {
    		errorMsgFromPhase1 = s2VoteFailed;
    	} else if (s1VoteFailed != null && s2VoteFailed == null) {
    		errorMsgFromPhase1 = s1VoteFailed;
    	} else if (s1VoteFailed != null && s2VoteFailed != null) {
    		errorMsgFromPhase1 = new KVMessage("ERROR", s1VoteFailed.getMessage() +s2VoteFailed.getMessage());
    	}
    	
    	SlaveDecisionRunnable sd1 = new SlaveDecisionRunnable(slave1, isCommit);
    	SlaveDecisionRunnable sd2 = new SlaveDecisionRunnable(slave2, isCommit);
    	s1Thread = new Thread(sd1);
    	s2Thread = new Thread(sd2);
    	s1Thread.start();
    	s2Thread.start();
    	
       	try {
    		s1Thread.join();
    		s2Thread.join();
    	} catch (InterruptedException e) {
    		masterCacheLock.unlock();
    		throw new KVException("Internal: TPCRequest Failed @ PHASE2: Interrupted exception");
    	}
       	
       	if (isCommit) {
       		if (isPutReq) {
       			masterCache.put(key,  value);
       		} else {
       			masterCache.del(key);
       		}
       	}  else {
       		masterCacheLock.unlock();
       		
       		throw new KVException(new KVMessage(RESP, "Your request can not be has been declined by slaves, might due to the key does not exist or format error"));
       	}
   		masterCacheLock.unlock();   	
    }
    
//+chen
    public class SlaveVoteRunnable implements Runnable {  	
    	TPCSlaveInfo slaveInfo;
    	String key, val;
    	boolean isPut;   	
    	KVMessage voteFailed = null;
    	KVMessage response = null;//Message type should be ABORT_VOTE or READY
    	boolean isAbort = false;
    	
    	public SlaveVoteRunnable(TPCSlaveInfo slaveInfo, String key, String val, boolean isPut) {
    		this.slaveInfo = slaveInfo;
    		this.key = key;
    		this.val = val;
    		this.isPut = isPut;
    	}
    	public void run() {
    		try {
    			if (isPut) {
    				response = slaveInfo.TPCPutVote(key, val);//socket connnection to slave servers
    				if (response.getMsgType().equals(ABORT_VOTE)) {
    					isAbort = true;
    				}
    			} else {
    				response = slaveInfo.TPCDeleteVote(key);
    				if (response.getMsgType().equals(ABORT_VOTE)) {
    					isAbort = true;
    				}
    			}
    		} catch (KVException e) {
    			System.out.println("Slave vote in phase 1 exception: " + e.getMsg().getMessage()); 
    			voteFailed = e.getMsg();
    		}
    }
    }
    
    public class SlaveDecisionRunnable implements Runnable {
    	TPCSlaveInfo slaveInfo;
    	boolean isCommit;
    	
    	public SlaveDecisionRunnable(TPCSlaveInfo slaveInfo, boolean isCommit) {
    		this.slaveInfo = slaveInfo;
    		this.isCommit = isCommit;
    	}
    	public void run() {
    		if (isCommit) {
    				slaveInfo.TPCCommit();	//Keep running until get an ACK from slave
    		} else {
    				slaveInfo.TPCAbort();//Keep running until get an ACK from slave
    		}
    	}
    }
    //-chen
    
    
    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    
    public String handleGet(KVMessage msg) throws KVException {
    	String key = msg.getKey();
    	String value = null;
    	KVMessage errorMsg = null;
    	
    	TPCSlaveInfo slave1 = findFirstReplica(key);

    	WriteLock masterCacheLock = masterCache.getWriteLock(key);
    	masterCacheLock.lock();
    	value = masterCache.get(key);
    	if (value != null) {
    		masterCacheLock.unlock();
    		return value;
    	}
    	
    	try {
			value = slave1.get(key);
			
			if (value == null) {
				TPCSlaveInfo slave2 = findSuccessor(slave1);
				value = slave2.get(key);
			}
			return value;		
		} catch (KVException e) {
			errorMsg = e.getMsg();
			System.out.println(errorMsg);
			throw e;
		} finally {
			masterCacheLock.unlock();
		}  		 	
    }
    
    
    

    private WriteLock slavesInfoLock = new ReentrantReadWriteLock().writeLock();
    //+chen
    public boolean registerFinished() {
    	slavesInfoLock.lock();
    	try {
    		return slaves.size() == numSlaves;
    	} finally {
    		slavesInfoLock.unlock();
    	}
    
    }

}
