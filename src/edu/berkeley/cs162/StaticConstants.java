package edu.berkeley.cs162;


public class StaticConstants {
	public static final int TIME_OUT = 2000;
	
	//KVMessage types
	public static final String RESP = "resp";
	public static final String PUT_REQ = "putreq";
	public static final String GET_REQ = "getreq";
	public static final String DEL_REQ = "delreq";
	public static final String REGISTER = "register";
	public static final String ABORT_VOTE = "abort_vote";
	public static final String READY = "ready";
	public static final String COMMIT = "commit";
	public static final String ABORT_DECISION = "abort_decision";
	public static final String ACK = "ack";

	//ERRORS
	public static final String SOCKET_ERROR =  "Socket error";
	public static final String MSG_TYPE_ERROR = "Message type is not supported";
	public static final String MSG_FORMAT_ERROR = "Message provided does not obey the designed format";
	public static final String SEND_MSG_FAILED= "Message could not be sent";

	public static final String KEY_INVALID_ERROR = "Key can not be null, empty or oversized";
	public static final String VAL_INVALID_ERROR = "Value  can not be null, empty or oversized";
	
	public static final String REQ_FORMAT_ERROR = "Request format is incorrect";
	
	public static final String ERROR_SOCKET_TIMEOUT = "Socket timeout";
	public static final String ERROR_COULD_NOT_CONNECT = "Socket could not connect";
	public static final String ERROR_COULD_NOT_CREATE_SOCKET = "Socket could not be created";
	
	public static final String ERROR_INVALID_FORMAT = "Slave info format incorrect";
	
	public static final String  INVALID_RESPONSE= "The response  is invalid";
	
	//OTHER INFO
	public static final String SUCCESS = "Success";
	
}

