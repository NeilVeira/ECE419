package app_kvEcs;

import java.io.*;
import java.util.*;

import org.apache.log4j.Level;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import common.HashRing;
import common.HashRing.Server;
import common.messages.*;
import client.Client;

public class ECS {
	private static Logger logger = Logger.getRootLogger();
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
	public ECS(String configFile) throws IOException, FileNotFoundException, Exception{
		this.configFile = new File(configFile);
		allServers = new ArrayList<Server>();
		allProcesses = new ArrayList<Process>();
		totalNumNodes = 0;
		new LogSetup("logs/ecs.log", Level.INFO); //TODO: change to WARN at the end

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
		catch (NumberFormatException e) {
			logger.error("Error! All ports in config file must be integers");
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
		logger.info("Initializing service");
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
			logger.info("Launching server "+server.toString());
			
			//launch the server
			String jarPath = new File(System.getProperty("user.dir"), "ms2-server.jar").toString();
			String launchCmd = "java -jar "+jarPath+" "+server.port+" "+cacheSize+" "+replacementStrategy+" "+indices[i]; 
			String sshCmd = "ssh -n localhost nohup "+launchCmd;
			
			//This is a temporary workaround because the ssh doesn't work
			//TODO: use ssh 
			try {
				Process p = Runtime.getRuntime().exec(launchCmd);
				allProcesses.set(indices[i],  p);
				metadata.addServer(server);
			}
			catch (IOException e){
				logger.warn("Warning: Unable to launch server "+server.toString());
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
		logger.info("Sending all servers metadata "+metadata.toString());
		ArrayList<Client> clients = getActiveServers();
		for (Client client : clients) {
			KVMessage message = new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString());
			System.out.println("message.validityCheck() "+message.validityCheck());
			client.sendMessage(message);
			KVMessage response = client.getResponse();
			if (!response.getStatus().equals("SUCCESS")){
				logger.warn("A server did not successfully update its metadata");
			}			
			client.closeConnection();
		}
	}
	
	/**
	 * Launch the storage servers chosen by initService
	 */
	public void start() {
		//TODO
		logger.info("Starting");
	}
	
	/**
	 * Stops all running servers in the service
	 */
	public void stop() {
		//TODO
		logger.info("Stopping");
	}
	
	/**
	 * Stops all servers and shuts down the ECS client
	 */
	public void shutDown() {
		logger.info("Shutting down");
		int i=0;
		for (Process p : allProcesses) {
			if (p != null){
				logger.info("Killing server "+allServers.get(i).ipAddress+" "+allServers.get(i).port);
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
		logger.info("Adding node "+cacheSize+" "+replacementStragey);
	}
	
	/**
	 * Remove the node with the given index
	 */
	public void removeNode(int index) {
		//TODO
		logger.info("Removing node "+index);
	}
	
	/**
	 * Get list of Client objects for every active server in the current metadata (i.e.
	 * servers which can be connected to successfully). 
	 * Any servers which can't be connected to are removed from the metadata.
	 */
	public ArrayList<Client> getActiveServers() {
		List<Server> activeServers = metadata.getAllServers();
		ArrayList<Client> clients = new ArrayList<Client>();
		for (Server server : activeServers) {
			//try connecting to this server 
			boolean success;
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
				logger.warn("Warning: Unable to connect to server "+server.toString());
			}
			else {
				logger.info("Connection successful to server "+server.toString());
			}
		}
		return clients;
	}
	
}