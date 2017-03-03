package common;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.*;

/**
 * Class to manage the mapping of servers to hash ranges
 * and the hash ring. 
 */
public class Metadata{
	private MessageDigest hasher;
	private TreeMap<BigInteger,Server> hashRing;
	
	public Metadata(){
		try{
			this.hasher = MessageDigest.getInstance("MD5");
			this.hashRing = new TreeMap<BigInteger,Server>();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Return a 128-bit (BigInteger) MD5 hash of the given IP address and port
	 * @return the hash value
	 */
	public BigInteger serverHash(Server server){
		hasher.update(server.ipAddress.getBytes());
		byte[] portBytes = ByteBuffer.allocate(4).putInt(server.port).array(); //convert int to byte array
		hasher.update(portBytes); 
		byte[] hash = hasher.digest();
		hasher.reset();
		return new BigInteger(hash);
	}
	
	public BigInteger objectHash(String key){
		hasher.update(key.getBytes());
		byte[] hash = hasher.digest();
		hasher.reset();
		return new BigInteger(hash);
	}
	
	/**
	 * Add the given server to the hash ring
	 */
	public void addServer(Server server){
		BigInteger hash = serverHash(server);
		hashRing.put(hash, server);
	}
	
	/**
	 * Remove the given server from the metadata
	 * @param server
	 */
	public void removeServer(Server server){
		BigInteger hash = serverHash(server);
		hashRing.remove(hash);
	}
	
	/**
	 * Return the server responsible for the data key or null if the 
	 * ring is empty. (NOT TESTED YET!)
	 */
	public Server getResponsible(String key){
		BigInteger keyHash = objectHash(key);
		Map.Entry<BigInteger,Server> entry = hashRing.ceilingEntry(keyHash);
		if (entry == null){
			//key is past the last server - wrap around to first
			entry = hashRing.firstEntry();
			if (entry == null){
				//hashRing is empty
				return null;
			}
		}
		return entry.getValue();
	}
	
	/**
	 * Encapsulates the IP address and port fields used by the Metadata class
	 *
	 */
	public class Server{
		public String ipAddress;
		public int port;
		
		public Server(String ipAddress, int port){
			this.ipAddress = ipAddress;
			this.port = port;
		}
	}
}

