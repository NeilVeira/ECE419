package common.messages;

import java.io.Serializable;
import java.util.*;

/**
 * Implementation of KVMessage interface which is used to store messages between
 * any client and server. These messages are used to between the kvclient and kvserver
 * as well as for admin messages between the ECS and the kvservers.  
 * How to use this class:
 * 		- Create a message using MessageType(header,status,key,value)
 * 		- Make sure that error = null
 * 		- convert to byte array using getBytes()
 * Converting from a byte array:
 * 		- MessageType(bytes)
 * 		- Make sure that error = null
 * 		- Use getHeader(), getStatus, getKey(), etc. *
 * Supported message types:
 * 		- get: tells kvserver to do a get operation
 * 		- put: tells kvserver to do a put operation
 * 		- metadata: tells kvserver to update its metadata with the metadata contained in the message
 *  	- connect: connect to server
 *  	- disconnect: disconnect from server
 *  	- logLevel: for changing the kvClient log verbosity
 *  	- quit: for exiting the kvClient
 *  	- help: print help
 */
public class MessageType implements KVMessage {
	public String error;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	protected String key;
	protected String value;
	protected String header;
	protected String status;
	
	/**
	 * Replace all single quotes in the string with double quotes
	 */
	public static String doubleQuotes(String s)
	{
		for (int i=0; i<s.length(); i++){
			if (s.charAt(i) == '"'){
				s = s.substring(0,i) + "\"\"" + s.substring(i+1,s.length());
				i++;
			}
		}
		return s;
	}
	
	/**
	 * Replace all double quotes in the string with single quotes
	 */
	public static String singleQuotes(String s)
	{
		for (int i=0; i<s.length()-1; i++){
			if (s.charAt(i) == '"' && s.charAt(i+1) == '"'){
				s = s.substring(0,i) + s.substring(i+1,s.length());
			}
		}
		return s;
	}
	
	/**
	 * Construct a MessageType with the 4 required fields. All fields should be 
	 * given with single quotes. Empty strings are acceptable now. 
	 */
	public MessageType(String header, String status, String key, String value)
	{
		this.header = header;
		this.status = status;
		this.key = key;
		this.value = value;
		this.error = validityCheck();
	}

	/***
	Construct MessageType from a byte array (ASCII-coded).
	***/
	public MessageType(byte[] bytes) {
		this.msgBytes = bytes;
		String str = new String(this.msgBytes);
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		parse(str.trim());
	}	

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	@Override
	public String getKey() {
		return this.key;
	}

	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	@Override
	public String getValue() {
		return this.value;
	}

	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	@Override
	public String getStatus() {
		return this.status;
	}
	
	@Override
	public void setStatus(String status) {
		this.status = status;
	}
		
	/**
	 * 
	 * @return a header string that is used to identify the message type
	 */
	@Override
	public String getHeader() {
		return this.header;
	}
	
	/**
	 * Returns the content of this message as a String. All fields (header, status,
	 * key, value) are surrounded by quotes. Any quotes in these values are replaced
	 * with double quotes.
	 */
	public String getMsg() {
		return 	"\""+doubleQuotes(header)+"\" " +
				"\""+doubleQuotes(status)+"\" " + 
				"\""+doubleQuotes(key)+"\" " + 
				"\""+doubleQuotes(value)+"\"";
	}

	/***
	Returns an array of bytes that represent the ASCII coded message content.
	Byte array is terminated by a '\n' character.
	***/
	public byte[] getMsgBytes() {
		this.msgBytes = toByteArray(this.getMsg());
		return this.msgBytes;
	}
	
	/**
	 * Returns the given string as an ASCII-coded byte array
	 */
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	/**
	 * Parse the given string into the header, status, key, and value fields.
	 * The given string MUST have the following format:
	 * "header" "status" "key" "value"
	 * where each of those fields can have quotes, but doubled. 
	 */
	public void parse(String msg){
		msg = msg.trim();
		List<String> tokens = new ArrayList<String>();
		
		//iterate though string character-by-character. 
		//inData - boolean flag indicating whether the current character is part of a data field or between data fields
		boolean inData = false; 
		int start = -1;
		for (int i=0; i<msg.length(); i++){
			if (!inData){
				if (msg.charAt(i) == '"'){
					inData = true;
					start = i;
				}
			}
			else{
				if (msg.charAt(i) == '"'){
					//data field ends when current character is quote but next is not
					if (i+1 == msg.length() || msg.charAt(i+1) != '"'){
						inData = false;
						tokens.add(msg.substring(start+1,i));
					}
					else{
						//next character is quote. Double quotes only occur in data field
						i++; //don't process 2nd quote of double pair
					}
				}
			}
		}
		
		if (tokens.size() != 4){
			this.error = "Invalid message format";
			return;
		}
		this.header = singleQuotes(tokens.get(0));
		this.status = singleQuotes(tokens.get(1));
		this.key = singleQuotes(tokens.get(2));
		this.value = singleQuotes(tokens.get(3));
		
		this.error = validityCheck();
	}

	/**
	 * Check that the message is valid, i.e. all the required fields are non-empty
	 * @return a string with the error message, or null if there are no errors
	 */
	public String validityCheck()
	{
		switch (header) {
		case "connect": 
		case "put":
			//use IP address and port as key & value
			if (key.trim().equals("") || value.trim().equals("")){
				return "Key and value must not be empty for message "+header;
			}
			break;
		case "logLevel":
			System.out.println(key.trim());
			if (!key.trim().equals("ALL") && !key.trim().equals("DEBUG") && !key.trim().equals("INFO") && !key.trim().equals("WARN") && !key.trim().equals("ERROR") && !key.trim().equals("FATAL") && !key.trim().equals("OFF")) {
				return "Log level must be equal to a valid log level.";
			}
			break;
		case "get":
			if (key.trim().equals("")){
				return "Key must not be empty for message "+header;
			}
			break;
		case "disconnect":
		case "help":
		case "quit":
			if (!key.trim().equals("") || !value.trim().equals("")){
				return "Key and value must be empty for message "+header;
			}
			break;
		case "metadata":
			if (value.trim().equals("") && !status.equals("SUCCESS")) {
				return "Value must not be empty for message "+header;
			}
			break;
		default:
			return "Unknown command";
		}
		
		if (key.length() > 20){
			return "Key must not be longer than 20 bytes";
		}
		return null; 
	}
}
