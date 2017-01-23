/***
Implementation of KVMessage interface which is used to store messages between
the client and server. 
Does the parsing of strings into header, status, key, and value.
Converts between messages and byte arrays. 
***/
package common.messages;

import java.io.Serializable;


public class MessageType implements KVMessage {
	private static final long serialVersionUID = 5549512212003782618L;
	private String msg;
	private byte[] msgBytes;
	private static final char LINE_FEED = 0x0A;
	private static final char RETURN = 0x0D;
	private String key;
	private String value;
	private String header;
	private String status;
	public boolean isValid;
	public String error_msg;
	
	/***
	Construct MessageType from a string. The string must have the format
	<header> <status> <key> <value>
	header must be one of connect,disconnect,get,put,quit,help,loglevel
	status should only be included if the argument include_status is true.
	key and value are also optional
	header, status, and key must not have whitespace.
	***/
	public MessageType(String msg, boolean include_status) {
		this.msg = msg;
		this.msgBytes = toByteArray(msg);
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		this.isValid = parse(msg, include_status);
	}

	/***
	Construct MessageType from a byte array (ASCII-coded). The status is 
	always assumed to be included in the message. 
	***/
	public MessageType(byte[] bytes) {
		this.msgBytes = addCtrChars(bytes);
		this.msg = new String(msgBytes);
		this.header = "";
		this.status = "";
		this.key = "";
		this.value = "";
		this.isValid = parse(this.msg, true);
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
	 * Returns the content of this message as a String.
	 */
	public String getMsg() {
		StringBuilder msg = new StringBuilder();
		//why doesn't string.join work???
		msg.append(header);
		msg.append(" ");
		msg.append(status);
		msg.append(" ");
		msg.append(key);
		msg.append(" ");
		msg.append(value);
		this.msg = msg.toString();
		return this.msg;
	}

	/***
	Returns an array of bytes that represent the ASCII coded message content.
	***/
	public byte[] getMsgBytes() {
		this.msgBytes = toByteArray(this.msg);
		return this.msgBytes;
	}
	
	private byte[] addCtrChars(byte[] bytes) {
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private byte[] toByteArray(String s){
		byte[] bytes = s.getBytes();
		byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
		byte[] tmp = new byte[bytes.length + ctrBytes.length];
		
		System.arraycopy(bytes, 0, tmp, 0, bytes.length);
		System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);
		
		return tmp;		
	}
	
	private boolean parse(String msg, boolean include_status){		
		String[] tokens = msg.split("\\s+");
		if (tokens.length == 0){
			this.error_msg = "Unknown command";
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
				this.status = "PUT";
				break;
			case "get":
				expected_num_tokens = 2;
				this.status = "GET";
				break;
			case "loglevel":
				expected_num_tokens = 2;
				break;
			case "help":
				expected_num_tokens = 1;
				break;
			case "quit":
				expected_num_tokens = 1;
				break;
			default:
				this.error_msg = "Unknown command";
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
				this.error_msg = "Incorrect number of tokens for message " + tokens[0];
				return false;
			}
		}
		else{
			if (tokens.length != expected_num_tokens){
				this.error_msg = "Incorrect number of tokens for message " + tokens[0];
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
