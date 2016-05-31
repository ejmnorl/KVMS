/**
 * XML Parsing library for the key-value store
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
import static edu.berkeley.cs162.StaticConstants.*;

import java.io.BufferedOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.StringWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;

import javax.xml.parsers.*;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 * This is the object that is used to generate messages the XML based messages 
 * for communication between clients and servers. 
 */
public class KVMessage implements Serializable{
	private String msgType = null;
	private String key = null;
	private String value = null;
	private String status = null;
	private String message = null;
	
	//+chen
	   private static final int MAX_KEY_SIZE = 256;
	   private static final int MAX_VAL_SIZE = 256 * 1024;
	   
	   private static final int TIMEOUT = 2000;
	//-chen
	
	public final String getKey() {
		return key;
	}

	public final void setKey(String key) {
		this.key = key;
	}

	public final String getValue() {
		return value;
	}

	public final void setValue(String value) {
		this.value = value;
	}

	public final String getStatus() {
		return status;
	}

	public final void setStatus(String status) {
		this.status = status;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

	public String getMsgType() {
		return msgType;
	}

	/* Solution from http://weblogs.java.net/blog/kohsuke/archive/2005/07/socket_xml_pitf.html */
	private class NoCloseInputStream extends FilterInputStream {
	    public NoCloseInputStream(InputStream in) {
	        super(in);
	    }
	    
	    public void close() {} // ignore close
	}
	
	//+chen
	private final static Set<String> _types;
	private final static void fillSet(Set<String> s) {
		s.add(GET_REQ);
		s.add("delreq");
		s.add("putreq");
		s.add("resp");
		//.....
		s.add("ready");
		s.add("abort_vote");	
		s.add("commit");
		s.add("abort_decision");
		s.add("ack");
		
		s.add("register");
	}
	
	static {
		final Set<String> typesTmp = new HashSet<String>();
		fillSet(typesTmp);
		_types = Collections.unmodifiableSet(typesTmp);
	}
	//-chen
	
	/***
	 * 
	 * @param msgType
	 * @throws KVException of type "resp" with message "Message format incorrect" if msgType is unknown
	 */
	public KVMessage(String msgType) throws KVException {
	    // TODO: implement me
		if (!_types.contains(msgType)) {
			System.out.println("***" + msgType);
			System.out.println(_types.contains(msgType));
			throw new KVException(new KVMessage("resp", "!!!msg type is not correct"));
		}
		this.msgType = msgType;
	}
	
	public KVMessage(String msgType, String message) throws KVException {
        // TODO: implement me	
		this(msgType);
		this.message = message;
	}
	
	public KVMessage(Socket sock) throws KVException {
		this(sock, TIMEOUT);
	}
	
	public KVMessage(Socket sock, int timeout) throws KVException {
		InputStream input = null;
		try {
			sock.setSoTimeout(timeout);
			input = sock.getInputStream();
		} catch ( IOException e) {
			System.err.println(e.getMessage());
			throw new KVException(SOCKET_ERROR);
		}
		
		String msgType;	
		NoCloseInputStream is = new NoCloseInputStream(input);	
		DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
		try {
			Document doc = fac.newDocumentBuilder().parse(is);		
			doc.getDocumentElement().normalize();
			
			NodeList nl = doc.getElementsByTagName("KVMessage");
			if (nl == null || nl.getLength() != 1) {
				throw new KVException(MSG_FORMAT_ERROR);
			}
			
			NamedNodeMap msgAttri = nl.item(0).getAttributes();
			if (msgAttri == null || msgAttri.getLength() != 1 || !msgAttri.item(0).getNodeName().equals("type")) {
				throw new KVException(MSG_FORMAT_ERROR);
			}
			
			msgType = msgAttri.item(0).getNodeValue();
			if (!_types.contains(msgType)) {
				throw new KVException(MSG_TYPE_ERROR);
			}
			this.msgType = msgType;
						
			NodeList keyNL, valueNL, msgNL;
			keyNL = doc.getElementsByTagName("Key");
			valueNL = doc.getElementsByTagName("Value");
			msgNL = doc.getElementsByTagName("Message");
			
			//Node keyN, valueN, messageN;		
			switch(msgType) {
			case PUT_REQ:
				checkNodeList(keyNL);
				this.key = keyNL.item(0).getTextContent();
				checkKey(this.key);
				
				checkNodeList(valueNL);
				this.value = valueNL.item(0).getTextContent();
				checkValue(this.value);
								
				break;
			case GET_REQ:
			case DEL_REQ:
				checkNodeList(keyNL);
				this.key = keyNL.item(0).getTextContent();
				checkKey(this.key);
				break;	
				
			case RESP:
			case ABORT_VOTE:
				if (msgNL.getLength() != 0) {
					checkNodeList(msgNL);
					this.message = msgNL.item(0).getTextContent();
				} else { //this is resp of "successful getreq"
					checkNodeList(keyNL);
					checkNodeList(valueNL);
					this.key = keyNL.item(0).getTextContent();
					this.value = valueNL.item(0).getTextContent();
					checkKey(this.key);
					checkValue(this.value);
				}
				break;
				
			case REGISTER:
				if (msgNL.getLength() != 0) {
					checkNodeList(msgNL);
					this.message = msgNL.item(0).getTextContent();
				}
				break;
				
			case READY:
			case COMMIT:
			case ABORT_DECISION:
			case ACK:				
				break;
				
			default:				
				throw new KVException(MSG_FORMAT_ERROR);		
			}
			
		} catch (SAXException | IOException | ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	 /***
     * Parse KVMessage from incoming network connection
     * @param sock
     * @throws KVException if there is an error in parsing the message. The exception should be of type "resp and message should be :
     * a. "XML Error: Received unparseable message" - if the received message is not valid XML.
     * b. "Network Error: Could not receive data" - if there is a network error causing an incomplete parsing of the message.
     * c. "Message format incorrect" - if there message does not conform to the required specifications. Examples include incorrect message type. 
     */
	public KVMessage(InputStream input) throws KVException {
		String msgType;
		NoCloseInputStream is = new NoCloseInputStream(input);
		DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
		try {
			Document doc = fac.newDocumentBuilder().parse(is);	
			NodeList nl = doc.getElementsByTagName("KVMessage");
			if (nl == null || nl.getLength() != 1) {
				throw new KVException(MSG_FORMAT_ERROR);
			}
			
			NamedNodeMap msgAttri = nl.item(0).getAttributes();
			if (msgAttri == null || msgAttri.getLength() != 1 || !msgAttri.item(0).getNodeName().equals("type")) {
				throw new KVException(MSG_FORMAT_ERROR);
			}
			
			msgType = msgAttri.item(0).getNodeValue();
			if (!_types.contains(msgType)) {
				throw new KVException(MSG_FORMAT_ERROR);
			}
			this.msgType = msgType;
						
			NodeList keyNL, valueNL, msgNL;
			keyNL = doc.getElementsByTagName("Key");
			valueNL = doc.getElementsByTagName("Value");
			msgNL = doc.getElementsByTagName("Message");
			
			//Node keyN, valueN, messageN;		
			switch(msgType) {
			case PUT_REQ:
				checkNodeList(keyNL);
				this.key = keyNL.item(0).getTextContent();
				checkKey(this.key);
				
				checkNodeList(valueNL);
				this.value = valueNL.item(0).getTextContent();
				checkValue(this.value);
								
				break;
			case GET_REQ:
			case DEL_REQ:
				checkNodeList(keyNL);
				this.key = keyNL.item(0).getTextContent();
				checkKey(this.key);
				break;	
				
			case RESP:
			case ABORT_VOTE:
				if (msgNL.getLength() != 0) {
					checkNodeList(msgNL);
					this.message = msgNL.item(0).getTextContent();
				} else { //this is resp of "successful getreq"
					checkNodeList(keyNL);
					checkNodeList(valueNL);
					this.key = keyNL.item(0).getTextContent();
					this.value = valueNL.item(0).getTextContent();
					checkKey(this.key);
					checkValue(this.value);
				}
				break;
			case REGISTER: 
				if (msgNL.getLength() != 0) {
					checkNodeList(msgNL);
					this.message = msgNL.item(0).getTextContent();
				}
				break;
			case READY:
			case COMMIT:
			case ABORT_DECISION:
			case ACK:
				break;
			default:				
				throw new KVException(new KVMessage(MSG_FORMAT_ERROR));		
			}
			
		} catch (SAXException | IOException | ParserConfigurationException e) {
			e.printStackTrace();
		}
	}
	
	private void checkNodeList(NodeList nl) throws KVException {
		if (nl == null || nl.getLength() != 1){
			throw new KVException(MSG_FORMAT_ERROR);
		}
	}
	
	private void checkKey(String key) throws KVException {
		if (key == null || key.length() == 0 || key.length() > MAX_KEY_SIZE) {
			throw new KVException(KEY_INVALID_ERROR);
		}
	}
	
	private void checkValue(String value) throws KVException {
		if (value == null || value.length() == 0 || value.length() > MAX_VAL_SIZE) {
			throw new KVException(VAL_INVALID_ERROR);	
		}
	}
		
	/**
	 * Generate the XML representation for this message.
	 * @return the XML String
	 * @throws KVException if not enough data is available to generate a valid KV XML message
	 */
	public String toXML() throws KVException {
        //return null;
	      // TODO: implement me
		Element rootE = null, keyE = null, valueE = null, messageE = null;
		String xml = null;
		
		try {
			DocumentBuilderFactory fac = DocumentBuilderFactory.newInstance();
			Document doc = fac.newDocumentBuilder().newDocument();
			doc.setXmlStandalone(true);
						
			rootE = doc.createElement("KVMessage");
			doc.appendChild(rootE);
			
			rootE.setAttribute("type", msgType);
			
			if (!_types.contains(msgType)) {
				throw new KVException( MSG_TYPE_ERROR);
			}
			switch(msgType) {
			case PUT_REQ:
				try {
					checkKey(key);
					keyE = doc.createElement("Key");
					keyE.appendChild(doc.createTextNode(key));
					rootE.appendChild(keyE);
									
					checkValue(value);
					valueE = doc.createElement("Value");
					valueE.appendChild(doc.createTextNode(value));
					rootE.appendChild(valueE);
				} catch (KVException e) {
					throw new KVException(REQ_FORMAT_ERROR);
				}
						
				if (message != null) {
					throw new KVException(REQ_FORMAT_ERROR);
				}
				
				break;
			case GET_REQ:
			case DEL_REQ:
				try {
					checkKey(key);
					keyE = doc.createElement("Key");
					keyE.appendChild(doc.createTextNode(key));
					rootE.appendChild(keyE);
				} catch (KVException e) {
					throw new KVException(REQ_FORMAT_ERROR);
				}

				if (value != null || message != null) {
					throw new KVException(REQ_FORMAT_ERROR);
				}
				
				break;
			case RESP:
			case ABORT_VOTE:
				if (this.message != null) {
					messageE = doc.createElement("Message");
					messageE.appendChild(doc.createTextNode(message));
					rootE.appendChild(messageE);				
					if (key != null || value != null) {
						throw new KVException(MSG_FORMAT_ERROR);
					}					
				} else {
					try {
						checkKey(key);
						keyE = doc.createElement("Key");
						keyE.appendChild(doc.createTextNode(key));
						rootE.appendChild(keyE);
										
						checkValue(value);
						valueE = doc.createElement("Value");
						valueE.appendChild(doc.createTextNode(value));
						rootE.appendChild(valueE);
					} catch (KVException e){
						throw new KVException(MSG_FORMAT_ERROR);
					}
		
					if (message != null) {
						throw new KVException(MSG_FORMAT_ERROR);
					}
				}				
				break;
				
			case REGISTER:
				if (message != null) {
					messageE = doc.createElement("Message");
					messageE.appendChild(doc.createTextNode(message));
					rootE.appendChild(messageE);
				} else {
					throw new KVException(MSG_FORMAT_ERROR);
				}
				break;

				
			case READY:
			case COMMIT:
			case ABORT_DECISION:
			case ACK:
				break;
				
			default:	
					throw new KVException(MSG_TYPE_ERROR);
			}			
			//convert DOM to String
			StringWriter stringWriter = new StringWriter();
			Transformer transformer;
			
				transformer = TransformerFactory.newInstance().newTransformer();
				transformer.transform(new DOMSource(doc), new StreamResult(stringWriter));
				transformer.setOutputProperty(OutputKeys.INDENT, "yes");
				xml = stringWriter.toString();
				
		} catch (ParserConfigurationException | TransformerFactoryConfigurationError | TransformerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
	 }
		return xml;
	}
	
	public void sendMessage(Socket sock) throws KVException {
		OutputStream out = null;
		try {
			out = new BufferedOutputStream(sock.getOutputStream());//Line1
			String msg = toXML();
			System.out.println("XML MESSAGE IS: " + msg);
			out.write(msg.getBytes(), 0, msg.length());
			System.out.println("wroten");
			out.flush();
			System.out.println("flushed");
			sock.shutdownOutput(); //Line 2
			System.out.println("socket closed");
		} catch (IOException e) {	//Might be thrown out by line1 or out.write or sock.shutdownOutput();		
			if (out != null) {
				try {
					out.close();					
				} catch (IOException e1) {
					throw new KVException(e1.getMessage());
				}
			}
			System.err.println(e.getMessage());
			throw new KVException(SOCKET_ERROR);
		} 
	}
}
