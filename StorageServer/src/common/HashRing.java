package common;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.*;

/**
 * Class to manage the mapping of servers to hash ranges
 * and the hash ring. 
 */
public class HashRing{
	private MessageDigest hasher;
	private TreeMap<BigInteger,Server> serverMap;
	
	public HashRing(){
		try{
			this.hasher = MessageDigest.getInstance("MD5");
			this.serverMap = new TreeMap<BigInteger,Server>();
		}
		catch (Exception e){
			e.printStackTrace();
		}
	}
	
	/**
	 * Construct a HashRing object from a string of data (created 
	 * from HashRing.toString()).
	 * Parses the data and loads it into the hash ring.
	 */
	public HashRing(String data) {
		try{
			this.hasher = MessageDigest.getInstance("MD5");
			this.serverMap = new TreeMap<BigInteger,Server>();
		}
		catch (Exception e){
			e.printStackTrace();
		}
		

		//parse data and load into serverMap
		String[] servers = data.split(",");
		for (String server : servers){
			String[] tokens = server.split("\\s+");
			assert (tokens.length == 3);
			BigInteger hash = new BigInteger(tokens[0]);
			int port = Integer.parseInt(tokens[2]);
			Server newServer = new Server(tokens[1], port);
			serverMap.put(hash, newServer);
		}
	}
	
	/**
	 * Return a 128-bit (BigInteger) MD5 hash of the given IP address and port
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
		serverMap.put(hash, server);
	}
	
	/**
	 * Remove the given server from the hash ring
	 */
	public void removeServer(Server server){
		BigInteger hash = serverHash(server);
		serverMap.remove(hash);
	}
	
	/**
	 * Return the server responsible for the data key or null if the 
	 * ring is empty. 
	 */
	public Server getResponsible(String key){
		BigInteger keyHash = objectHash(key);
		Map.Entry<BigInteger,Server> entry = serverMap.ceilingEntry(keyHash);
		if (entry == null){
			//key is past the last server - wrap around to first
			entry = serverMap.firstEntry();
			if (entry == null){
				//serverMap is empty
				return null;
			}
		}
		return entry.getValue();
	}
	
	/**
	 * Convert the entire mapping to a String. String is formatted as a comma-delimited
	 * list of entries, where each entry is a space-delimited list of the form 
	 * "<hash> <IP address> <port>"
	 */
	public String toString() {
		String ret = "";
		for (Map.Entry<BigInteger,Server> entry : serverMap.entrySet()){
			ret += entry.getKey().toString() + " ";
			ret += entry.getValue().ipAddress + " ";
			ret += String.valueOf(entry.getValue().port);
			ret += ",";
		}
		
		if (ret.length() > 0){
			//strip off trailing comma
			ret = ret.substring(0, ret.length()-1);
		}
		return ret;
	}
	
	/**
	 * Encapsulates the IP address and port fields used by the HashRing class.
	 */
	public static class Server{
		public String ipAddress;
		public int port;
		
		public Server(String ipAddress, int port){
			this.ipAddress = ipAddress;
			this.port = port;
		}
	}
}
