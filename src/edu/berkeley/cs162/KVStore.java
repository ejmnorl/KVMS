/**
 * Persistent Key-Value storage layer. Current implementation is transient, 
 * but assume to be backed on disk when you do your project.
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
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.*;
import org.xml.sax.SAXException;


/**
 * This is a dummy KeyValue Store. Ideally this would go to disk, 
 * or some other backing store. For this project, we simulate the disk like 
 * system using a manual delay.
 *
 */
public class KVStore implements KeyValueInterface {
	private Dictionary<String, String> store 	= null;
	
	public KVStore() {
		resetStore();
	}

	private void resetStore() {
		store = new Hashtable<String, String>();
	}
	
	public boolean put(String key, String value) throws KVException {
		AutoGrader.agStorePutStarted(key, value);
		
		try {
			putDelay();
			store.put(key, value);
			return false;
		} finally {
			AutoGrader.agStorePutFinished(key, value);
		}
	}
	
	public String get(String key) throws KVException {
		AutoGrader.agStoreGetStarted(key);
		
		try {
			getDelay();
			System.out.println("@ KVStore : get");
			String retVal = this.store.get(key);
			if (retVal == null) {
			    KVMessage msg = new KVMessage("resp", "key \"" + key + "\" does not exist in store");
			    throw new KVException(msg);
			}
			return retVal;
		} finally {
			AutoGrader.agStoreGetFinished(key);
		}
	}
	
	public void del(String key) throws KVException {
		AutoGrader.agStoreDelStarted(key);

		try {
			delDelay();
			if(key != null)
				this.store.remove(key);//?????????What if this key does not exist???, It will be handled @kvServer
		} finally {
			AutoGrader.agStoreDelFinished(key);
		}
	}
	
	private void getDelay() {
		AutoGrader.agStoreDelay();
	}
	
	private void putDelay() {
		AutoGrader.agStoreDelay();
	}
	
	private void delDelay() {
		AutoGrader.agStoreDelay();
	}
	
    public String toXML() throws KVException {
        // TODO: implement me
        //return null;
    	try {
			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			Document doc = db.newDocument();
			doc.setXmlStandalone(true);
			Element root = doc.createElement("KVStore");
			doc.appendChild(root);
			
			ArrayList<String> keys = Collections.list(store.keys());
			
			Collections.sort(keys);
			
			Element KVPairNode,  keyNode, valueNode;
			for (String key : keys) {
				KVPairNode = doc.createElement("KVPair");
				keyNode = doc.createElement("Key");
				valueNode = doc.createElement("Value");
				
				keyNode.setTextContent(key);
				valueNode.setTextContent(store.get(key));
				
				root.appendChild(KVPairNode);
				KVPairNode.appendChild(keyNode);
				KVPairNode.appendChild(valueNode);
				
			}
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
	    		Transformer transformer = transformerFactory.newTransformer();
	    	    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
	    		StringWriter writer = new StringWriter();
	    		transformer.transform(new DOMSource(doc), new StreamResult(writer));
	    		return writer.getBuffer().toString();
	
    	} catch (Exception e) {
    		//System.err.println("KVStore::dumpToString: Exception building DOM: " + e);
    		throw new KVException(new KVMessage("resp", "KVStore::dumpToString: Exception building DOM: " + e));
    	}
    	//return "";
    }        

    public void dumpToFile(String fileName) throws KVException {
        // TODO: implement me
    	try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(new File(fileName)));
			bw.write(this.toXML());
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void restoreFromFile(String fileName) throws KVException{
        // TODO: implement me
    	try {
    		DocumentBuilder db =  DocumentBuilderFactory.newInstance().newDocumentBuilder();
    		Document doc = db.parse(new File(fileName));
    		
    		NodeList nl = doc.getElementsByTagName("KVPair");
    		int n = nl.getLength();
    		String key, value;
    		resetStore();
    		
    		for (int i = 0; i < n; i++) {
    			Node cur = nl.item(i);
    			if (cur.getNodeType() == Node.ELEMENT_NODE) {
    				Element curE = (Element) cur;
    				Node keyN = curE.getElementsByTagName("Key").item(0);
    				key = keyN.getTextContent();
    				Node valueN = curE.getElementsByTagName("Value").item(0);
    				value = valueN.getTextContent();
    				store.put(key,  value);
    			}		
    		}
    	} catch (IOException |  ParserConfigurationException | SAXException e) {
    		//throw new KVException(new KVMessage("resp", "restore from file failed!"));
    		System.err.println("KVStore::restoreFromFile: " + e);
    	}
    }
}
