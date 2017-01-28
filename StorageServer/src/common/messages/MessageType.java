/***
Implementation of KVMessage interface which is used to store messages between
the client and server. 
Does the parsing of strings into header, status, key, and value.
Converts between messages and byte arrays. 
***/
package common.messages;

import java.io.Serializable;


public class MessageType implements KVMessage {
	public String originalMsg;
	public boolean isValid;
	public String error;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	private String key;
	private String value;
	private String header;
	private String status;
	
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
	
	public MessageType(String header, String status, String key, String value)
	{
		this.header = header;
		this.status = status;
		this.key = key;
		this.value = value;
	}
	
	/***
	Construct MessageType from a string. The string must have the format
	<header> <status> <key> <value>
	header must be one of connect,disconnect,get,put,quit,help,loglevel
	status should only be included if the argument include_status is true.
	key and value are also optional
	header, status, and key must not have whitespace.
	***/
	public MessageType(String msg, boolean include_status) { //would be nice to give include_status default value of false... how to do in java?
		this.originalMsg = msg.trim();
		this.msgBytes = toByteArray(msg);
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		this.isValid = parse(this.originalMsg, include_status);
	}
	
	/**
	 * Same as above but include_status is false by default
	 */
	public MessageType(String msg) { 
		this.originalMsg = msg.trim();
		this.msgBytes = toByteArray(msg);
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		this.isValid = parse(this.originalMsg, false);
	}

	/***
	Construct MessageType from a byte array (ASCII-coded).
	***/
	public MessageType(byte[] bytes, boolean include_status) {
		this.msgBytes = bytes;
		this.originalMsg = new String(this.msgBytes);
		this.originalMsg.trim();
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		this.isValid = parse(this.originalMsg, include_status);
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
		getMsg(); //reconstruct msg string
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
	 key, value) are surrounded by quotes. Any quotes in these values are replaced
	 with double quotes.
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
	
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private boolean parse(String msg, boolean include_status){		
		String[] tokens = msg.split("\\s+");
		if (tokens.length == 0){
			this.error = "Unknown command";
			return false;
		}
		this.header = tokens[0];
		StringBuilder val = new StringBuilder();
		int expected_num_tokens;
		
		switch (this.header) {
			case "connect": 
				expected_num_tokens = 3;
				break;
			case "disconnect":
				expected_num_tokens = 1;
				break;
			case "put":
				expected_num_tokens = 3;
				break;
			case "get":
				expected_num_tokens = (include_status ? 3 : 2); //special case when constructed with status - should contain a value returned by the server
				break;
			case "logLevel":
				expected_num_tokens = 2;
				break;
			case "help":
				expected_num_tokens = 1;
				break;
			case "quit":
				expected_num_tokens = 1;
				break;
			default:
				this.error = "Unknown command";
				return false;
		}
		if (include_status){
			//need additional token for status
			expected_num_tokens++;
		}
		
		//check for correct number of tokens. Put is a special case because it can have any number of tokens
		//(but at least 3)
		if (tokens[0].equals("put")){
			if (tokens.length < expected_num_tokens){
				this.error = "Incorrect number of tokens for message " + tokens[0];
				return false;
			}
		}
		else{
			if (tokens.length != expected_num_tokens){
				this.error = "Incorrect number of tokens for message " + tokens[0];
				return false;				
			}
		}
		
		if (include_status){
			this.status = tokens[1];
		}
		//key is index 1, or 2 if it includes status
		int key_idx = 1 + (include_status ? 1 : 0);
		if (tokens.length >= 2){
			this.key = tokens[key_idx];			
		}
		//remainder is value
		for (int i=key_idx+1; i < tokens.length; i++){
			val.append(tokens[i]);
			if (i != tokens.length -1 ) {
				val.append(" ");
			}
		}
		this.value = val.toString();
		
		return true;
	}

}
