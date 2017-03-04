package app_kvEcs;

import java.io.*;
import java.util.*;

import org.apache.zookeeper.ZooKeeper;

import common.HashRing;
import common.HashRing.Server;

public class ECS {
	private File configFile;
	private ZooKeeper zookeeper;
	private HashRing metadata;
	private List<Server> allServers; //array of all servers in the system. This never changes.
	private List<Process> allProcesses; //array of all processes in the system, one for each server (can be null if server is not running).
	private String launchScript = "launch_server.sh";
	private int totalNumNodes;
	
	/**
	 * Creates a new ECS instance with the servers in the given config file. 
	 */
	public ECS(String configFile){
		this.configFile = new File(configFile);
		allServers = new ArrayList<Server>();
		allProcesses = new ArrayList<Process>();
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
		
		for (int i=0; i<totalNumNodes; i++){
			allProcesses.add(null);
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
			
			//launch the server
			String jarPath = new File(System.getProperty("user.dir"), "ms2-server.jar").toString();
			String launchCmd = "java -jar "+jarPath+" "+server.port+" "+cacheSize+" "+replacementStrategy; 
			String sshCmd = "ssh -n localhost nohup "+launchCmd;
			
			//This is a temporary workaround because the ssh doesn't work
			//TODO: use ssh 
			Process p = Runtime.getRuntime().exec(launchCmd);
			allProcesses.set(indices[i],  p);
			
			metadata.addServer(server);
			
			connectString += server.ipAddress+":"+String.valueOf(server.port)+",";
		}
		
		if (connectString.length() > 0){
			//strip off trailing comma
			connectString = connectString.substring(0,connectString.length()-1);
		}
		System.out.println("Zookeeper connect string: "+connectString);
		
		//TODO: initialize zookeeper with connectString
		
		//TODO: connect to each server and send them the metadata
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
		int i=0;
		for (Process p : allProcesses) {
			if (p != null){
				System.out.println("Killing server "+allServers.get(i).ipAddress+" "+allServers.get(i).port);
				p.destroy();
			}
			i++;
		}
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