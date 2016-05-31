/**
 * Slave Server component of a KeyValue store
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

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * This class defines the slave key value servers. Each individual KVServer 
 * would be a fully functioning Key-Value server. For Project 3, you would 
 * implement this class. For Project 4, you will have a Master Key-Value server 
 * and multiple of these slave Key-Value servers, each of them catering to a 
 * different part of the key namespace.
 *
 */
public class KVServer implements KeyValueInterface {
	/*
	public static void main(String[] args) {
		try {
			test();
		} catch (KVException e) {
			System.out.println("Aha! Catched exception ");
		}
	}
	
	private static void test() throws KVException {
		for (int i = 0; i < 5; i++) {
			System.out.println("i = " + i);
			if (i == 4) {
				throw new KVException(new KVMessage("resp", " hehe"));
			}
			System.out.println("TEST HERE");//This one can not be reached
		}
	}
	*/
	
	private KVStore dataStore = null;
	private KVCache dataCache = null;
	
	private static final int MAX_KEY_SIZE = 256;
	private static final int MAX_VAL_SIZE = 256 * 1024;
	
	/**
	 * @param numSets number of sets in the data Cache.
	 */
	public KVServer(int numSets, int maxElemsPerSet) {
		dataStore = new KVStore();
		dataCache = new KVCache(numSets, maxElemsPerSet);	
		AutoGrader.registerKVServer(dataStore, dataCache);
	}
	
	public KVStore getStore() {
		return dataStore;
	}
	
	public KVCache getCache() {
		return dataCache;
	}
	
	public boolean put(String key, String value) throws KVException {
		validateKey(key);
		validateValue(value);
		
		WriteLock writeLock = dataCache.getWriteLock(key);
		try {
			writeLock.lock();
		synchronized(dataStore) {
				try {
					dataStore.put(key,  value);
					dataCache.put(key, value);
				} catch (KVException e) {
					throw new KVException(new KVMessage("resp", "put data into store and cache failed!"));
				}
			}
		} finally {
			// Must be called before returning
			AutoGrader.agKVServerPutFinished(key, value);
			if (writeLock.isHeldByCurrentThread()) {
				writeLock.unlock();
			}
		}		
		return false;
	}
	
	public String get (String key) throws KVException {
		WriteLock lock = dataCache.getWriteLock(key);
		String value = null;
		try {
			validateKey(key);
			lock.lock();
			value = dataCache.get(key);
			if (value == null) {
				synchronized(dataStore) {
					try {
						System.out.println("bbbbbbbbb");
						value = dataStore.get(key);
						System.out.println("afafafafa");
					} catch (KVException e) {
						System.out.println("@ KVServer : The key to get does not exist!, throw out exp");
						throw new KVException(new KVMessage("resp", "The data requested does not exist!"));
					}
					dataCache.put(key, value);
				}
			}
		} finally {
			if (lock.isHeldByCurrentThread()) {
				lock.unlock();
			}
		}
		return value;
	}
	
	public void del (String key) throws KVException {
		// Must be called before anything else
		WriteLock lock = dataCache.getWriteLock(key);
		lock.lock();
		
		synchronized(dataStore) {			
			try {
				validateKey(key);
				dataStore.del(key);
				dataCache.del(key);
			} catch (KVException e) {
				throw new KVException(new KVMessage("resp", "Del failed!"));
			} finally {
				// Must be called before returning
				AutoGrader.agKVServerDelFinished(key);				
				if (lock.isHeldByCurrentThread()) {
					lock.unlock();
				}
			}
		}	
	}
	

	private void validateKey(String key) throws KVException {
		if (key == null || key.length() == 0) {
			throw new KVException(new KVMessage("resp", "Key is empty!"));
		} else if (key.length() > MAX_KEY_SIZE) {
			throw new KVException(new KVMessage("resp", "Key oversized!"));
		} 		
	}
	
	private void validateValue(String value) throws KVException {
		if (value == null || value.length() == 0) {
			throw new KVException(new KVMessage("resp","Value is empty!"));
		} else if (value.length() > MAX_VAL_SIZE) {
			throw new KVException(new KVMessage("resp", "Value oversized!"));
		}
	}
	
}
