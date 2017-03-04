package app_kvEcs;

import java.io.*;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import common.HashRing;
import common.HashRing.Server;
import common.messages.*;
import client.Client;

public class ECS {
	private File configFile;
	private ZooKeeper zookeeper;
	private HashRing metadata;
	private List<Server> allServers; //array of all servers in the system. This never changes.
	private List<Process> allProcesses; //array of all processes in the system, one for each server (can be null if server is not running).
	private String launchScript = "launch_server.sh";
	private int totalNumNodes;
	private Logger logger = Logger.getRootLogger();
	
	/**
	 * Creates a new ECS instance with the servers in the given config file. 
	 */
	public ECS(String configFile) {
		// Argument is the path of the configuration file (ecs.config)
		this.configFile = new File(configFile);
		allServers = new ArrayList<Server>();
		allProcesses = new ArrayList<Process>();
		// Initialize node (servers) number to zero, increment for each line of config file
		totalNumNodes = 0;

		try{
			String currentLine;
			BufferedReader FileReader = new BufferedReader(new FileReader(this.configFile));
			while ((currentLine = FileReader.readLine()) != null) {
				// Config file in format of "server_name server_address port"
				// Each line is a server
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
		
		for (int i=0; i < totalNumNodes; i++) {
			// Add an empty process for each node
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
		
		// Validity checking for arguments
		if (numberOfNodes <= 0) {
			throw new Exception("Number of nodes must be a positive integer");
		}
		if (numberOfNodes > totalNumNodes) {
			throw new Exception("Cannot initialize service with "+numberOfNodes+" nodes."
					+" Only "+totalNumNodes+" known nodes exist.");
		}
		if (!replacementStrategy.equals("FIFO") && !replacementStrategy.equals("LRU") && !replacementStrategy.equals("LFU")) {
			throw new Exception("Invalid replacement strategy "+replacementStrategy+". Only FIFO, LRU, and LFU are accepted.");
		}
			
		// Generate numberOfNodes random indices from 1 to n
		Integer[] indices = new Integer[totalNumNodes];
		for (int i=0; i < indices.length; i++){
			indices[i] = i;
		}
		// Randomize the list of indices
		Collections.shuffle(Arrays.asList(indices)); 
		
		for (int i=0; i<numberOfNodes; i++){
			Server server = allServers.get(indices[i]);
			System.out.println("Launching server "+server.ipAddress+" "+server.port);
			
			// Launch the server
			String jarPath = new File(System.getProperty("user.dir"), "ms2-server.jar").toString();
			String launchCmd = "java -jar "+jarPath+" "+server.port+" "+cacheSize+" "+replacementStrategy; 
			String sshCmd = "ssh -n localhost nohup "+launchCmd;
			
			// Use ssh to launch servers
			try {
				Process p = Runtime.getRuntime().exec(sshCmd);
				allProcesses.set(indices[i],  p);
				metadata.addServer(server);
			}
			catch (IOException e){
				System.out.println("Warning: Unable to ssh to server "+server.toString());
			}
			
			connectString += server.ipAddress+":"+String.valueOf(server.port)+",";
		}
		
		if (connectString.length() > 0){
			//strip off trailing comma
			connectString = connectString.substring(0,connectString.length()-1);
		}
		System.out.println("Zookeeper connect string: "+connectString);
		
		//TODO: initialize zookeeper with connectString
		
		//connect to each server and send them the metadata
		ArrayList<Client> clients = getActiveServers();
		for (Client client : clients) {
			KVMessage message = new MessageType("metadata","METADATA_UPDATE",metadata.toString(),"");
			client.sendMessage(message);
			KVMessage response = client.getResponse();
			//TODO: make sure this is METADATA_ACK status
			
			client.closeConnection();
		}
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
				try {
					Process killing_p = Runtime.getRuntime().exec("ssh -n localhost nohup fuser -k " + allServers.get(i).port + "/tcp");
				} catch (IOException e) {
					System.out.println(e.getMessage());
				}
				p.destroy();
			}
			i++;
		}
	}
	
	/**
	 * Creates a new server with the given cache size and replacement strategy
	 * and adds it to the service
	 */
	public void addNode(int cacheSize, String replacementStrategy) {
		//TODO
		System.out.println("Adding node "+cacheSize+" "+replacementStrategy);
	}
	
	/**
	 * Remove the node with the given index
	 */
	public void removeNode(int index) {
		//TODO
		System.out.println("Removing node "+index);
	}
	
	/**
	 * Get list of Client objects for every active server in the current metadata (i.e.
	 * servers which can be connected to successfully). 
	 * Any servers which can't be connected to are removed from the metadata.
	 */
	public ArrayList<Client> getActiveServers() {
		List<Server> activeServers = metadata.getAllServers();
		ArrayList<Client> clients = new ArrayList<Client>();
		for (int i = 0; i < activeServers.size(); ++i) {
			Server server = activeServers.get(i);
			//try connecting to this server 
			boolean success = false;
			try {
				Client client = new Client(server.ipAddress, server.port);
				//wait for "connection successful" response
				KVMessage response = client.getResponse();
				if (response != null){
					success = true;
					clients.add(client);
				}
				else{
					success = false;
				}
			}
			catch (Exception e){
				success = false;
			}
			
			if (!success) {
				metadata.removeServer(server);
				System.out.println("Warning: Unable to connect to server "+server.toString());
			}
			else {
				System.out.println("Connection successful to server "+server.toString());
			}
		}
		return clients;
	}
	
}