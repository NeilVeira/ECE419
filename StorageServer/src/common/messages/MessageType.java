/**
 * 
 */
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
	private StatusType status;
	public boolean isValid;
	public String error_msg;

    /**
     * Constructs a MessageType object with a given array of bytes that 
     * forms the message.
     * 
     * @param bytes the bytes that form the message in ASCII coding.
     */
	public MessageType(byte[] bytes) {
		msgBytes = addCtrChars(bytes);
		msg = new String(msgBytes);
		isValid = false;
		status = null;
		this.isValid = parse();
	}
	
	/**
     * Constructs a MessageType object with a given String that
     * forms the message. 
     * 
     * @param msg the String that forms the message.
     */
	public MessageType(String msg) {
		this.msg = msg;
		msgBytes = toByteArray(msg);
		isValid = false;
		status = null;
		this.isValid = parse();
	}
	
	@Override
	public String getKey() {
		return this.key;
	}


	@Override
	public String getValue() {
		return this.value;
	}

	@Override
	public StatusType getStatus() {
		return this.status;
	}
	
	@Override
	public void setStatus(StatusType status) {
		this.status = status;
	}
	
	@Override
	public String getHeader() {
		return this.header;
	}
	
	
	/**
	 * Returns the content of this TextMessage as a String.
	 * 
	 * @return the content of this message in String format.
	 */
	public String getMsg() {
		return msg;
	}

	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 * 
	 * @return the content of this message as an array of bytes 
	 * 		in ASCII coding.
	 */
	public byte[] getMsgBytes() {
		return msgBytes;
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
	
	private boolean parse(){
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
				break;
			case "get":
				expected_num_tokens = 2;
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
		/*if ((tokens[0] != "put" && tokens.length != expected_num_tokens) || (tokens[0] == "put" && tokens.length < expected_num_tokens)){
			this.error_msg = "Incorrect number of tokens for message " + tokens[0];
			return false;
		}*/
		
		if (tokens.length >= 2){
			this.key = tokens[1];			
		}
		for (int i=2; i<tokens.length; i++){
			val.append(tokens[i]);
			if (i != tokens.length -1 ) {
				val.append(" ");
			}
		}
		this.value = val.toString();
		
		return true;
	}

}
