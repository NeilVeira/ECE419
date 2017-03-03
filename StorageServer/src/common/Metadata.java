package common;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.math.BigInteger;

/**
 * Class to manage the mapping of servers to hash ranges
 *
 */
public class Metadata{
	private MessageDigest hasher;
	
	public Metadata(){
		try{
			this.hasher = MessageDigest.getInstance("MD5");
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Return a 128-bit (BigInteger) MD5 hash of the given IP address and port
	 * @param ipAddress
	 * @param port
	 * @return
	 */
	public BigInteger getHash(String ipAddress, int port){
		hasher.update(ipAddress.getBytes());
		byte[] portBytes = ByteBuffer.allocate(4).putInt(port).array(); //convert int to byte array
		hasher.update(portBytes); 
		byte[] hash = hasher.digest();
		hasher.reset();
		return new BigInteger(hash);
	}
	
	public void addServer(String ipAddress, int port){
		//TODO
	}
	
	public void removeServer(String ipAddress, int port){
		//TODO
	}
	
	public Server getResponsible(String key){
		//TODO
		return new Server("",0);
	}
	
	/**
	 * Encapsulates the IP address and port fields used by the Metadata class
	 *
	 */
	public class Server{
		public String ipAddress;
		public int port;
		
		Server(String ipAddress, int port){
			this.ipAddress = ipAddress;
			this.port = port;
		}
	}
}

