package app_kvEcs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.*;

import org.apache.zookeeper.ZooKeeper;

import common.HashRing;
import common.HashRing.Server;

public class ECS {
	private File configFile;
	private ZooKeeper zookeeper;
	private HashRing metadata;
	private List<Server> allServers; //array of all servers in the system. This never changes.
	private String launchScript = "launch_server.sh";
	private int totalNumNodes;
	
	/**
	 * Creates a new ECS instance with the servers in the given config file. 
	 */
	public ECS(String configFile){
		this.configFile = new File(configFile);
		allServers = new ArrayList<Server>();
		totalNumNodes = 0;

		try{
			String currentLine;
			BufferedReader FileReader = new BufferedReader(new FileReader(this.configFile));
			while ((currentLine = FileReader.readLine()) != null) {
				String[] tokens = currentLine.split(" ");
				int port = Integer.parseInt(tokens[2]);
				allServers.add(new Server(tokens[1], port));
				totalNumNodes++;
			}			
			
		}
		catch (FileNotFoundException e) {
			System.out.println("Error! Could not find config file "+this.configFile);
		}
		catch (NumberFormatException e) {
			System.out.println("Error! All ports in config file must be integers");
		}
		catch (Exception e){
			System.out.println("Error encountered creating ECS!");
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Reads and parses the ecs config file, launches the servers, and initializes
	 * the zookeeper object. 
	 */
	public void initService(int numberOfNodes, int cacheSize, String replacementStrategy) throws Exception {
		System.out.println("Initializing service");
		metadata = new HashRing();
		String connectString = ""; //comma-separated list of "IP address:port" pairs for zookeeper
		
		//validity checking for arguments
		if (numberOfNodes <= 0){
			throw new Exception("Number of nodes must be a positive integer");
		}
		if (numberOfNodes > totalNumNodes){
			throw new Exception("Cannot initialize service with "+numberOfNodes+" nodes."
					+" Only "+totalNumNodes+" known nodes exist.");
		}
		if (!replacementStrategy.equals("FIFO") && !replacementStrategy.equals("LRU") && !replacementStrategy.equals("LFU")){
			throw new Exception("Invalid replacement strategy "+replacementStrategy);
		}
			
		//generate numberOfNodes random indices from 1 to n
		Integer[] indices = new Integer[totalNumNodes];
		for (int i=0; i<indices.length; i++){
			indices[i] = i;
		}
		Collections.shuffle(Arrays.asList(indices)); //randomize the list of indices
		
		for (int i=0; i<numberOfNodes; i++){
			Server server = allServers.get(indices[i]);
			System.out.println("Launching server "+server.ipAddress+" "+server.port);
			//TODO: run the ssh command/script
			
			metadata.addServer(server);
			
			connectString += server.ipAddress+":"+String.valueOf(server.port)+",";
		}
		
		if (connectString.length() > 0){
			//strip off trailing comma
			connectString = connectString.substring(0,connectString.length()-1);
		}
		System.out.println("Zookeeper connect string: "+connectString);
		
		//TODO: initialize zookeeper with connectString

	}
	
	/**
	 * Launch the storage servers chosen by initService
	 */
	public void start() {
		//TODO
		System.out.println("Starting");
	}
	
	/**
	 * Stops all running servers in the service
	 */
	public void stop() {
		//TODO
		System.out.println("Stopping");
	}
	
	/**
	 * Stops all servers and shuts down the ECS client
	 */
	public void shutDown() {
		stop();
		//TODO
		System.out.println("Shutting down");
	}
	
	/**
	 * Creates a new server with the given cache size and replacement strategy
	 * and adds it to the service
	 */
	public void addNode(int cacheSize, String replacementStragey) {
		//TODO
		System.out.println("Adding node "+cacheSize+" "+replacementStragey);
	}
	
	/**
	 * Remove the node with the given index
	 */
	public void removeNode(int index) {
		//TODO
		System.out.println("Removing node "+index);
	}
}