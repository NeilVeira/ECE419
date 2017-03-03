package common.messages;

public interface KVMessage {
	public String originalMsg=null;
	public boolean isValid=false;
	public String error=null;
	
	public enum StatusType {
		GET, 			/* Get - request */
		GET_ERROR, 		/* requested tuple (i.e. value) not found */
		GET_SUCCESS, 	/* requested tuple (i.e. value) found */
		PUT, 			/* Put - request */
		PUT_SUCCESS, 	/* Put - request successful, tuple inserted */
		PUT_UPDATE, 	/* Put - request successful, i.e. value updated */
		PUT_ERROR, 		/* Put - request not successful */
		DELETE_SUCCESS, /* Delete - request successful */
		DELETE_ERROR 	/* Delete - request not successful */
	}

	/**
	 * @return the key that is associated with this message, 
	 * 		null if not key is associated.
	 */
	public String getKey();
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue();
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public String getStatus();
	
	public void setStatus(String status);
	
	/**
	 * 
	 * @return a header string that is used to identify the message type
	 */
	public String getHeader();
	
	public String getMsg();
	
	/**
	 * Returns an array of bytes that represent the ASCII coded message content.
	 */
	public byte[] getMsgBytes();
	
	/**
	 * Check that the message is valid, i.e. all the required fields are non-empty
	 * @return a string with the error message, or null if there are no errors
	 */
	public String validityCheck(); 
	
}





