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
import app_kvServer.KVServer;

public class ECS {
	private static Logger logger = Logger.getRootLogger();
	private File configFile;
	private ZooKeeper zookeeper;
	private HashRing metadata;
	private List<Server> allServers; //array of all servers in the system. This never changes.
	private List<Process> allProcesses; //array of all processes in the system, one for each server (can be null if server is not running).
	private String launchScript = "launch_server.sh";
	private int totalNumNodes;
	private KVServer.ServerStatus status;
	private String metadataFile;
	
	/**
	 * Creates a new ECS instance with the servers in the given config file. 
	 */
	public ECS(String configFile) throws IOException, FileNotFoundException, Exception{  
		// Argument is the path of the configuration file (ecs.config)
		this.configFile = new File(configFile);
		allServers = new ArrayList<Server>();
		allProcesses = new ArrayList<Process>();
		// Initialize node (servers) number to zero, increment for each line of config file
		totalNumNodes = 0;
		status = KVServer.ServerStatus.STOPPED;
		metadataFile = "ecs_metadata.txt";

		try{
			String currentLine;
			BufferedReader FileReader = new BufferedReader(new FileReader(this.configFile));
			while ((currentLine = FileReader.readLine()) != null) {
				// Config file in format of "server_name server_address port"
				// Each line is a server
				String[] tokens = currentLine.split(" ");
				int port = Integer.parseInt(tokens[2]);
				allServers.add(new Server(tokens[1], port, totalNumNodes));
				totalNumNodes++;
			}			
			
		}
		catch (NumberFormatException e) {
			logger.error("Error! All ports in config file must be integers");
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
		
		logger.info("Initializing service");
		//String connectString = ""; //comma-separated list of "IP address:port" pairs for zookeeper
		
		//try to read the old metadata from the file
		try {
			BufferedReader FileReader = new BufferedReader(new FileReader(metadataFile));
			String data = FileReader.readLine();
			metadata = new HashRing(data);
		} 
		catch (Exception e){
			metadata = new HashRing();
			logger.warn("Could not load metadata from file");
		}

		//For proper persistency all the servers that were running when the ecs was last online
		//must be started. Then transfer all their data to the newly initialized nodes
		//with a series of add and remove operations. 
		List<Server> previousServers = metadata.getAllServers();
		for (Server server : previousServers) {
			runServer(server, cacheSize, replacementStrategy); 
		}
		broadcast(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
			
		// Generate numberOfNodes random indices from 1 to n
		Integer[] indices = new Integer[totalNumNodes];
		for (int i=0; i < indices.length; i++){
			indices[i] = i;
		}
		// Randomize the list of indices
		Collections.shuffle(Arrays.asList(indices)); 
		
		//run the first numberOfNodes indices
		for (int i=0; i<numberOfNodes; i++){
			int idx = indices[i];
			//check if this server is in previousServers (already running)
			//Do O(N^2) search because there's no way the number of servers will be so large that it matters.
			boolean found = false;
			for (Server server : previousServers) {
				if (server.id == idx){
					found = true;
					break;
				}
			}
			if (!found){
				addNode(idx, cacheSize, replacementStrategy);
				//Server server = allServers.get(idx);
				//runServer(server, cacheSize, replacementStrategy);
				//connectString += server.ipAddress+":"+String.valueOf(server.port)+",";
			}
		}
		
		//remove previous servers which are not among the new servers
		for (Server server : previousServers) {
			//Do O(N^2) search because there's no way the number of servers will be so large that it matters.
			boolean found = false;
			for (int i=0; i<numberOfNodes; i++) {
				if (server.id == indices[i]){
					found = true;
					break;
				}
			}
			if (!found){
				removeNode(server.id);
			}
		}
		
		/*if (connectString.length() > 0){
			//strip off trailing comma
			connectString = connectString.substring(0,connectString.length()-1);
		}
		logger.debug("Zookeeper connect string: "+connectString);*/
		
		//TODO: initialize zookeeper with connectString
		
		//connect to each server and send them the metadata
		KVMessage message = new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString());
		broadcast(message);
	}
	
	/**
	 * Launch the storage servers chosen by initService
	 */
	public void start() {
		logger.info("Starting");
		//Send a start command to all active servers.
		//TODO: This is not intended to be the final way of doing this. We should use a zookeeper
		//znode; this is just to get the functionality so we can move on for now.
		this.status = KVServer.ServerStatus.ACTIVE; 
		broadcast(new KVAdminMessage("start","","",""));
	}
	
	/**
	 * Stops all running servers in the service
	 */
	public void stop() {
		logger.info("Stopping");
		//TODO: This is not intended to be the final way of doing this. We should use a zookeeper
		//znode; this is just to get the functionality so we can move on for now.
		this.status = KVServer.ServerStatus.STOPPED;
		broadcast(new KVAdminMessage("stop","","",""));	
	}
	
	/**
	 * Stops all servers and shuts down the ECS client
	 */
	public void shutDown() {
		logger.info("Shutting down");
		for (int i=0; i<totalNumNodes; i++) {
			killServer(i);			
		}
	}
	
	/**
	 * Creates a new server with the given cache size and replacement strategy
	 * and adds it to the service. 
	 * Returns true on success.
	 */
	public boolean addRandomNode(int cacheSize, String replacementStrategy) {
		//TODO
		logger.info("Adding node "+cacheSize+" "+replacementStrategy);
		
		//determine which servers are not currently in the metadata
		//List<Server> currentServers = metadata.getAllServers();
		ArrayList<Integer> availableNodes = new ArrayList<Integer>();
		for (int id=0; id<totalNumNodes; id++){
			Server server = allServers.get(id);
			if (!metadata.contains(server)){
				availableNodes.add(server.id);
			}
		}
		if (availableNodes.size() == 0){
			logger.warn("There are no available nodes to add");
			return false;
		}
		
		//pick a random value from available servers
		Random random = new Random();
		int i = random.nextInt(availableNodes.size());
		int index = availableNodes.get(i);
		return addNode(index, cacheSize, replacementStrategy);
	}
	
	public boolean addNode(int index, int cacheSize, String replacementStrategy) {
		Server newServer = allServers.get(index);
		logger.info("Adding new server "+newServer.toString());
		
		runServer(newServer, cacheSize, replacementStrategy);
		
		//tell the successor server to transfer the data to the new server
		Server successor = metadata.getSuccessor(newServer);
		try {
			KVMessage response = sendSingleMessage(successor, new KVAdminMessage("addNode","",newServer.toString(),""));
			if (!response.getStatus().equals("SUCCESS")){
				logger.error("Unable to add server "+newServer.toString());
				return false;
			}
		}
		catch (Exception e) {
			logger.error("Unable to send moveData message to server "+successor.toString()+
					"Error: "+e.getMessage());
			return false;
		}
		
		// broadcast metadata update
		metadata.addServer(newServer);
		broadcast(new KVAdminMessage("metadata","","",metadata.toString()));
		
		//start the new node
		if (status ==  KVServer.ServerStatus.ACTIVE){
			try {
				sendSingleMessage(newServer, new KVAdminMessage("start","","",""));
			}
			catch (Exception e){
				logger.error("Unable to send start to new server. "+e.getMessage());
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Remove the node with the given index. This index if defined according
	 * to the initial configuration file. 
	 */
	public boolean removeNode(int index) {
		logger.info("Removing node "+index);
		//validity checking
		if (index < 0 || index >= totalNumNodes){
			logger.error("index "+index+" out of range");
			return false;
		}
		Server server = allServers.get(index);
		if (!metadata.contains(server)){
			logger.error("Server "+index+" is already offline");
			return false;
		}
		
		Server successor = metadata.getSuccessor(server);
		try {
			KVMessage response = sendSingleMessage(server, new KVAdminMessage("removeNode","",successor.toString(),server.toString()));
			if (!response.getStatus().equals("SUCCESS")){
				logger.error("Unable to remove server "+server.toString());
				return false;
			}
		}
		catch (Exception e) {
			logger.error("Unable to send moveData message to server "+successor.toString()+
					"Error: "+e.getMessage());
			return false;
		}
		
		//update metadata
		metadata.removeServer(server);
		broadcast(new KVAdminMessage("metadata","","",metadata.toString()));
		killServer(index);
		
		return true;
	}
	
	/**
	 * Get list of Client objects for every active server in the current metadata (i.e.
	 * servers which can be connected to successfully). 
	 * Any servers which can't be connected to are removed from the metadata.
	 */
	private ArrayList<Client> getActiveServers() {
		List<Server> activeServers = metadata.getAllServers();
		ArrayList<Client> clients = new ArrayList<Client>();
		for (int i = 0; i < activeServers.size(); ++i) {
			Server server = activeServers.get(i);
			//try connecting to this server 
			boolean success = false;
			try {
				logger.debug("Trying to connect to server "+server.toString());
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
				logger.debug(e.getMessage());
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
	
	/**
	 * Send the given message to all responsive servers in the metadata.
	 */
	private void broadcast(KVMessage message) {
		//connect to each server and send them the metadata
		logger.info("Broadcasting "+message.getMsg());
		ArrayList<Client> clients = getActiveServers();
		if (clients.size() == 0){
			logger.warn("There does not appear to be any servers online.");
		}
		for (Client client : clients) {
			try {
				client.sendMessage(message);
				KVMessage response = client.getResponse();
				if (!response.getStatus().equals("SUCCESS")) {
					logger.warn("A server did not successfully process the request "+message.toString());
				}	
			}
			catch (IOException e){
				logger.warn("A server did not successfully process the request "+message.toString());
			}
			client.closeConnection();
		}
	}
	
	private void runServer(Server server, int cacheSize, String replacementStrategy) {
		logger.info("Launching server "+server.toString());
		
		//Neil's hack: remove this later
		/*Scanner reader = new Scanner(System.in);  
		System.out.println("Paused. Enter a number: ");
		int n = reader.nextInt(); 
		metadata.addServer(server);*/
		
		// Launch the server
		String jarPath = new File(System.getProperty("user.dir"), "ms2-server.jar").toString();
		String launchCmd = "java -jar "+jarPath+" "+server.port+" "+cacheSize+" "+replacementStrategy+" "+server.id; 
		String sshCmd = "ssh -n localhost nohup "+launchCmd;
		
		// Use ssh to launch servers
		try {
			Process p = Runtime.getRuntime().exec(sshCmd);
			allProcesses.set(server.id,  p);
			metadata.addServer(server);
		}
		catch (IOException e){
			logger.warn("Warning: Unable to ssh to server "+server.toString()+". Error: "+e.getMessage());
		}
	}
	

	private void killServer(int id) {		
		Process p = allProcesses.get(id);
		Server server = allServers.get(id);
		if (p != null){
			logger.info("Killing server "+server.ipAddress+" "+server.port);
			
			/*//Neil's hack: remove this later
			Scanner reader = new Scanner(System.in);  
			System.out.println("Paused. Enter a number: ");
			int n = reader.nextInt(); */
			
			try {
				Process killing_p = Runtime.getRuntime().exec("ssh -n localhost nohup fuser -k " + allServers.get(id).port + "/tcp");
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
			p.destroy();
		}
	}
	
	/**
	 * Send a message to a specific server and return its response
	 */
	private KVMessage sendSingleMessage(Server server, KVMessage message) throws IOException {
		Client client = new Client(server.ipAddress, server.port);
		//wait for "connection successful" response
		KVMessage response = client.getResponse();
		
		//send message
		client.sendMessage(message);
		return client.getResponse();
	}
	
	/**
	 * Write metadata to metadata file.
	 */
	public void writeMetadata() {
		try {
			PrintWriter writer = new PrintWriter(metadataFile, "UTF-8");
			writer.println(metadata.toString());
			writer.close();
		}
		catch (Exception e) {
			logger.error("Could not write metadata to file");
		}
	}
}