/**
 * Implementation of a set-associative cache.
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

import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import javax.xml.parsers.*;
import javax.xml.transform.*;
import javax.xml.transform.dom.*;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;

/**
 * A set-associate cache which has a fixed maximum number of sets (numSets).
 * Each set has a maximum number of elements (MAX_ELEMS_PER_SET).
 * If a set is full and another entry is added, an entry is dropped based on the eviction policy.
 */
public class KVCache implements KeyValueInterface {	
	private int numSets = 100;
	private int maxElemsPerSet = 10;
	
	private List<cacheSet> _sets;	

	
	/**
	 * Creates a new LRU cache.
	 * @param cacheSize	the maximum number of entries that will be kept in this cache.
	 */
	public KVCache(int numSets, int maxElemsPerSet) {
		this.numSets = numSets;
		this.maxElemsPerSet = maxElemsPerSet;     
		// TODO: Implement Me!
		_sets = new LinkedList<cacheSet>();
		for(int i = 0; i < numSets; i++) {
			_sets.add(new cacheSet(i, maxElemsPerSet));
		}
	}

	/**
	 * Retrieves an entry from the cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key the key whose associated value is to be returned.
	 * @return the value associated to this key, or null if no value with this key exists in the cache.
	 */
	public String get(String key) {
		int id = getSetId(key);
		String ret =  _sets.get(id).get(key);
		
		return ret;
	}

	/**
	 * Adds an entry to this cache.
	 * If an entry with the specified key already exists in the cache, it is replaced by the new entry.
	 * If the cache is full, an entry is removed from the cache based on the eviction policy
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 * @param value	a value to be associated with the specified key.
	 * @return true is something has been overwritten 
	 */
	public boolean put(String key, String value) {
		int id = getSetId(key);
		_sets.get(id).put(key, value);
		
		return false;
	}

	/**
	 * Removes an entry from this cache.
	 * Assumes the corresponding set has already been locked for writing.
	 * @param key	the key with which the specified value is to be associated.
	 */
	public void del (String key) {
		// Must be called before anything else
		AutoGrader.agCacheGetStarted(key);
		AutoGrader.agCacheDelDelay();
		
		// TODO: Implement Me!
		int id = getSetId(key);
		_sets.get(id).delete(key);
		
		// Must be called before returning
		AutoGrader.agCacheDelFinished(key);
	}
	
	/**
	 * @param key
	 * @return	the write lock of the set that contains key.
	 */
	public WriteLock getWriteLock(String key) {
	    // TODO: Implement Me!
		int id = getSetId(key);
		return _sets.get(id).getWriteLock();
	}
	
	/**
	 * 
	 * @param key
	 * @return	set of the key
	 */
	private int getSetId(String key) {
		return Math.abs(key.hashCode()) % numSets;
	}
	
    public String toXML() {
        // TODO: Implement Me!
    	
    	try {
			DocumentBuilder builder =  DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = builder.newDocument();
			
			Element kvCacheNode = doc.createElement("KVCache");
			doc.appendChild(kvCacheNode);
			
			for (cacheSet set : _sets) {				
				Element setNode = doc.createElement("Set");
				setNode.setAttribute("Id", String.valueOf(set._id));
				kvCacheNode.appendChild(setNode);
				
				Element cacheEntryNode, keyNode, valueNode;
				for (int i = 0; i < set._size; i++) {
					cacheEntry entry = set._queue.get(i);
					cacheEntryNode = doc.createElement("CacheEntry");
					cacheEntryNode.setAttribute("isReferenced", String.valueOf(entry._isReferenced));
					
					keyNode = doc.createElement("Key");
					keyNode.setTextContent(entry._key);
					valueNode = doc.createElement("Value");
					valueNode.setTextContent(entry._value);
					
					setNode.appendChild(cacheEntryNode);
					cacheEntryNode.appendChild(keyNode);
					cacheEntryNode.appendChild(valueNode);
					
				}
				
			}
			
			Transformer transformer = TransformerFactory.newInstance().newTransformer();
			DOMSource domSource = new DOMSource(doc);
			Writer sw = new StringWriter();
			Result streamResult = new StreamResult(sw);
			transformer.transform(domSource, streamResult);
			return streamResult.toString();
			
		} catch (ParserConfigurationException | TransformerFactoryConfigurationError | TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        return null;
    }
    
    //+chen
    static class cacheEntry {    	
    	boolean _isReferenced;
    	 String _key;
    	 String _value;
    	
    	public cacheEntry(boolean isReferenced, String key, String value) {
    		_key = key;
    		_value = value;
    		_isReferenced = isReferenced;
    	}   	    	
    }
    
    static class cacheSet {
    	int _id;
    	int _size;
    	List<cacheEntry> _queue;
    	HashMap<String, cacheEntry> _quickCheck;
    	ReentrantReadWriteLock _lock;
    //	Lock readLock;
    //	Lock writeLock;
    	    	
    	//	
    	public void print() {
    		System.out.println("Print cache........ ");
    		int cnt = 0;
    	    		for (cacheEntry e :  _queue) {
    	    			System.out.println(" key: " + e._key + " " + "value : " + e._value);
    	    			cnt++;
    	    		}
    	    		System.out.println("total member: " + cnt);
    	}
    	//
    	
    	public cacheSet(int id, int size) {
    		_id = id;
    		_size = size;
    		_queue = new LinkedList<cacheEntry>();
    		_quickCheck = new HashMap<String, cacheEntry>();
    		_lock = new ReentrantReadWriteLock();
 //   		readLock = _lock.readLock();
 //   		writeLock = _lock.writeLock();
    	}
    	
    	public  void put(String key, String value) {  
 // 		writeLock.lock();
    		if (_quickCheck.containsKey(key)) {
    			cacheEntry cur = _quickCheck.get(key);
    			cur._isReferenced = true;
    			cur._value = value;
   // 			writeLock.unlock();
    			return;
    		}
    		
    	//	System.out.println("q size: " + _queue.size() + " key : " + key + " id : " + _id);
    		if (_queue.size() == _size) {//Current set is already full!!
    	//		System.out.println("@key: " + key + " EVICTION");
    			eviction();
    		}
    		//System.out.println("hello " + key);
    		cacheEntry newCE = new cacheEntry(false, key, value);
    		_quickCheck.put(key, newCE);
    		_queue.add(newCE);
    		//System.out.println("there " + key);
    		    		
    		//System.out.println("PUT PUT PUT DONE");
    //		for (cacheEntry e :  _queue) {
    //			System.out.print(" key: " + e._key + " " + " value : " + e._value);
    //		}
    //		System.out.println();
    //		writeLock.unlock();
    	}
    	
    	public String delete(String key) {
    		
 //   		writeLock.lock();
    		if (!_quickCheck.containsKey(key)) {
//    			writeLock.unlock();
    			return null;
    		}
    		cacheEntry toDelete = _quickCheck.remove(key);
    		_queue.remove(toDelete);
    		
  //  		for (cacheEntry e :  _queue) {
   // 			System.out.print("## key: " + e._key + " ");
   // 		}
   // 		System.out.println();
    //		writeLock.unlock();
    		return toDelete._key;
    	}
    	
    	public String get(String key) {
    		
   // 		readLock.lock();
    		if (!_quickCheck.containsKey(key)) {
  //  			readLock.unlock();
    			return null;
    		}
    		_quickCheck.get(key)._isReferenced = true;
   // 		readLock.unlock();
    		return _quickCheck.get(key)._value;
    	}
    	
    	private void eviction() {
 //   		writeLock.lock();
    		cacheEntry toDelete = null;
    		System.out.println("IN EVACTION");
    		int cnt = 0;
    		for (cacheEntry cur : _queue) {
    	//		System.out.println("Iteration : " + cnt);
    	//		cnt++;
    			if (!cur._isReferenced) {
    	//			System.out.println("@eviction: to evict: " + cur._key + " " + cur._value);
    				toDelete = cur;
    				break;
    			}
    		}
    		if (toDelete == null) {
    //			for (int i = 0; i < _queue.size(); i++) {
    //				System.out.print("iiii : " + _queue.get(i)._key + "@" + _queue.get(i)._value + "       ");
    //			}
    //			System.out.println();
    			
    			toDelete = _queue.get(0);
    //			System.out.println("GET 000000: " + toDelete._key);
    		//	toDelete = _queue;///////////////////
    		} 
    //		System.out.println("!!!!!!!!!!!! todelete: "  +  toDelete._key);
    		delete(toDelete._key);
   // 		writeLock.unlock();
    	}
    
    	WriteLock getWriteLock() {
    		return _lock.writeLock();
    	}
    	
    }
    //-chen
}
