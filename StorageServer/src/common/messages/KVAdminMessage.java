package common.messages;

/**
 * This class is essentially the same as MessageType but allows more valid 
 * message headers. 
 */
public class KVAdminMessage extends MessageType {
	
	/**
	 * Construct a KVAdminMessage with the 4 required fields. All fields should be 
	 * given with single quotes. Empty strings are acceptable now. 
	 */
	public KVAdminMessage(String header, String status, String key, String value)
	{
		super(header, status, key, value);
	}
	
	public KVAdminMessage(byte[] bytes) {
		super(bytes);
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
		case "admin_put":
		case "removeNode":
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
		case "addNode":
			if (key.trim().equals("")){
				return "Key must not be empty for message "+header;
			}
			break;
		case "disconnect":
		case "help":
		case "shutdown":
		case "init":
		case "start":
		case "stop":
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





