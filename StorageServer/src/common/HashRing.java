package common;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.math.BigInteger;
import java.util.*;

import client.Client;

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
	
	public void ClearHashRing() {
		this.hasher.reset();
		this.serverMap.clear();
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
			//ignore invalid servers (usually empty)
			if (tokens.length >= 3){
				BigInteger hash = new BigInteger(tokens[0]);
				int port = Integer.parseInt(tokens[2]);
				Server newServer = new Server(tokens[1], port);
				if (tokens.length >= 4){
					newServer.id = Integer.parseInt(tokens[3]);
				}
				serverMap.put(hash, newServer);
			}
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
	 * Returns the server which comes after the given one in the ring.
	 * If the given server is not in the ring, returns whichever servers comes
	 * after its hypothetical position if it were in the ring. 
	 */
	public Server getSuccessor(Server server) {
		BigInteger hash = serverHash(server);
		Map.Entry<BigInteger,Server> entry = serverMap.higherEntry(hash);
		if (entry == null){
			entry = serverMap.firstEntry();
			if (entry == null) {
				return null;
			}
		}
		return entry.getValue();
	}
	
	/**
	 * Return true if the given server is contained in the hash ring
	 */
	public boolean contains(Server server) {
		BigInteger hash = serverHash(server);
		return serverMap.containsKey(hash);
	}
	
	/**
	 * Convert the entire mapping to a String. String is formatted as a comma-delimited
	 * list of entries, where each entry is a space-delimited list of the form 
	 * "<hash> <IP address> <port>"
	 */
	public String toString() {
		String ret = "";
		for (Map.Entry<BigInteger,Server> entry : serverMap.entrySet()) {
			ret += entry.getKey().toString() + " ";
			ret += entry.getValue().ipAddress + " ";
			ret += String.valueOf(entry.getValue().port)+" ";
			ret += String.valueOf(entry.getValue().id);
			ret += ",";
		}
		
		if (ret.length() > 0){
			//strip off trailing comma
			ret = ret.substring(0, ret.length()-1);
		}
		return ret;
	}
	
	/**
	 * Returns a list of all servers in the ring
	 */
	public List<Server> getAllServers() {
		List<Server> allServers = new LinkedList<Server>();
		for (Map.Entry<BigInteger,Server> entry : serverMap.entrySet()) {
			allServers.add(entry.getValue());
		}
		return allServers;
	}

	/**
	 * Encapsulates the IP address and port fields used by the HashRing class.
	 */
	public static class Server{
		public String ipAddress;
		public int port;
		public int id;
		
		public Server(String ipAddress, int port){
			this.ipAddress = ipAddress;
			this.port = port;
			this.id = 0;
		}
		
		public Server(String ipAddress, int port, int id){
			this.ipAddress = ipAddress;
			this.port = port;
			this.id = id;
		}
		
		//construct from a string
		public Server(String str) {
			String[] tokens = str.split("\\s+");
			this.ipAddress = tokens[0];
			this.port = Integer.parseInt(tokens[1]);
			this.id = Integer.parseInt(tokens[2]);
		}
		
		public String toString(){
			return this.ipAddress+" "+this.port+" "+this.id;
		}
	}
}

