package app_kvEcs;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ZooKeeper;

import common.HashRing;
import common.HashRing.Server;
import common.messages.*;
import client.Client;
import app_kvServer.KVServer;
import app_kvServer.KVServer.ServerStatus;

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
	private String backupConfigFile;
	private File m_lockFile;
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
		backupConfigFile = "ecs_config_backup.txt";
		this.m_lockFile = new File("ECSMetadataLock.txt");
		this.metadata = new HashRing();

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
	
			checkConfig();
			writeConfig();
		}
		catch (NumberFormatException e) {
			logger.error("Error! All ports in config file must be integers");
		}
		
		for (int i=0; i < totalNumNodes; i++) {
			// Add an empty process for each node
			allProcesses.add(null);
		}
	}
	
	public void CreateLockFile() {
		try {
			this.m_lockFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public boolean CheckIfLockFileExist() {
		return this.m_lockFile.isFile();
	}
	
	public void RemoveLockFile() {
		this.m_lockFile.delete();
	}
	
	public HashRing getMetaData() {
		readMetadata();
		return this.metadata;
	}
	
	public void clearMetaData() {
		this.metadata.ClearHashRing();
		writeMetadata();
	}
	
	public List<Server> getAllServers() {
		return this.allServers;
	}
	
	/**
	 * Reads the previous configuration from the backup config file. 
	 * If that configuration is different from the current one, deletes the metadata file
	 * so that the system will restart from scratch.
	 */
	private void checkConfig() {
		boolean changed = false;
		try {
			//open file and parse it
			String currentLine;
			BufferedReader FileReader = new BufferedReader(new FileReader(backupConfigFile));
			List<Server> backupServers = new ArrayList<Server>();
			int id=0;
			while ((currentLine = FileReader.readLine()) != null) {
				backupServers.add(new Server(currentLine));
			}
			
			//check that backupServers = this.allServers
			if (backupServers.size() != allServers.size()){
				logger.warn("Config file appears to have changed since the ECS was last run. Restarting from fresh state.");
				changed = true;
			}
			else{
				for (id=0; id<allServers.size(); id++){
					Server a = allServers.get(id);
					Server b = backupServers.get(id);
					if (!a.ipAddress.equals(b.ipAddress) || a.port != b.port || a.id != b.id){
						logger.warn("Config file appears to have changed since the ECS was last run. Restarting from fresh state.");
						changed = true;
						break;
					}
				}
			}
			
			
		} catch (Exception e){
			logger.warn("Could not read backup config file. Starting from fresh state.");
			changed = true;
		}
		
		if (changed) {
			File file = new File(metadataFile);
			file.delete();
		}
	}
	
	/**
	 * Writes the current set of servers to the backup config file.
	 */
	public void writeConfig() {
		try {
			PrintWriter writer = new PrintWriter(backupConfigFile, "UTF-8");
			for (Server server : allServers){
				writer.println(server.toString());
			}
			writer.close();
		}
		catch (Exception e) {
			logger.warn("Could not backup config file");
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
			logger.warn("Could not load previous metadata from file");
		}

		//For proper persistency all the servers that were running when the ecs was last online
		//must be started. Then transfer all their data to the newly initialized nodes
		//with a series of add and remove operations. 
		List<Server> previousServers = metadata.getAllServers();
		if (previousServers.size() > 0) {
			for (Server server : previousServers) {
				runServer(server, cacheSize, replacementStrategy); 
			}			
			broadcast(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()), 10);
		}
			
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
			if (previousServers.size() > 0){
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
			else{
				//no previous servers. Forget about restoring state with addNode and just run it.
				runServer(allServers.get(idx), cacheSize, replacementStrategy);
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
		
		//connect to each server and send them the metadata
		KVMessage message = new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString());
		broadcast(message, 10);
		writeMetadata();
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
		broadcast(new KVAdminMessage("start","","",""), 3);
	}
	
	/**
	 * Stops all running servers in the service
	 */
	public void stop() {
		logger.info("Stopping");
		//TODO: This is not intended to be the final way of doing this. We should use a zookeeper
		//znode; this is just to get the functionality so we can move on for now.
		this.status = KVServer.ServerStatus.STOPPED;
		broadcast(new KVAdminMessage("stop","","",""), 3);	
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
	 * Randomly chooses an inactive server from the server pool, runs it with the 
	 * given cache size and replacement strategy, and adds it to the service. 
	 * Returns true on success.
	 * This is the method which should be used by the ECS client.
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
	
	/**
	 * Runs the node with the given index, cache size, and replacement strategy.
	 * Returns true on success.
	 * If the given server is already running this does nothing and returns false.
	 */
	public boolean addNode(int index, int cacheSize, String replacementStrategy) {
		Server newServer = allServers.get(index);
		//check if new server is already in the system
		if (metadata.contains(newServer)){
			return false;
		}
		
		logger.info("Adding new server "+newServer.toString());
		/*Scanner reader = new Scanner(System.in);
		System.out.println("Paused. Enter a number: ");		
		int n = reader.nextInt();*/
		
		runServer(newServer, cacheSize, replacementStrategy);
		
		//We musn't proceed from here unless the new server is online. Try to connect to it 
		//a few times to make sure it is. If after a certain number of tries it still isn't online
		//then abort.
		int numTries = 5;
		boolean success = false;
		for (int tries=0; tries<numTries && !success; tries++){
			try {
				Client client = new Client(newServer.ipAddress, newServer.port);
				success = true;
			}
			catch (IOException e) {
				logger.debug("Unable to connect to server "+newServer.toString()+". Waiting 1 second and trying again.");
				try {
					TimeUnit.SECONDS.sleep(1); 		
				} catch (InterruptedException ex){}
			}
		}
		if (!success) {
			logger.error("Unable to connect to server "+newServer.toString()+" after "+numTries+" tries. Aborting addNode.");
			metadata.removeServer(newServer); //this shouldn't be necessary, but there might be a bug... just in case.
			return false;
		}
		
		//tell the successor server to transfer the data to the new server
		Server successor = metadata.getSuccessor(newServer);
		logger.debug("Sending addNode message to successor "+successor.toString());
		try {
			KVMessage response = sendSingleMessage(successor, new KVAdminMessage("addNode","",newServer.toString(),""));
			if (!response.getStatus().equals("SUCCESS")){
				logger.error("Unable to add server "+newServer.toString());
				return false;
			}
		}
		catch (Exception e) {
			logger.error("Unable to send moveData message to server "+successor.toString()+
					". Error: "+e.getMessage());
			return false;
		}
		
		// broadcast metadata update
		metadata.addServer(newServer);
		broadcast(new KVAdminMessage("metadata","","",metadata.toString()), 5);
		
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
		
		/*reader = new Scanner(System.in);
		System.out.println("Done addNode. Enter a number: ");		
		n = reader.nextInt();*/
		
		writeMetadata();
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
		broadcast(new KVAdminMessage("metadata","","",metadata.toString()), 5);
		killServer(index);
		
		return true;
	}
	
	/**
	 * Get list of Client objects for every active server in the current metadata (i.e.
	 * servers which can be connected to successfully). 
	 * If a server can't be connected to, wait for 1 second and try again up to a maximum
	 * of numTries times.
	 * Any servers which can't still be connected to are removed from the metadata.
	 */
	private ArrayList<Client> getActiveServers(int numTries) {
		List<Server> activeServers = metadata.getAllServers();
		ArrayList<Client> clients = new ArrayList<Client>();
		boolean success=false;
		
		for (int i = 0; i < activeServers.size(); ++i) {
			Server server = activeServers.get(i);
			
			int triesRemaining = numTries;
			while (triesRemaining > 0){
				//try connecting to this server 
				success = false;
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
					triesRemaining--;
					if (triesRemaining > 0){
						logger.debug("Unable to connect to server "+server.toString()+". Waiting 1 second and trying again.");
						try {
							TimeUnit.SECONDS.sleep(1); 		
						} catch (InterruptedException e){}
					}
				}
				else {
					logger.info("Connection successful to server "+server.toString());
					break;
				}
			}

			//connected successfully or tried unsuccessfully numTries times
			if (!success) {
				triesRemaining--;
				metadata.removeServer(server);
				logger.error("Unable to connect to server "+server.toString()+" after "+numTries+" attempts.");
			}
			else {
				logger.info("Connection successful to server "+server.toString());
			}
		}
		
		return clients;
	}
	
	/**
	 * Send the given message to all responsive servers in the metadata.
	 * If unable to connect the the server, wait for 1 second and try again for 
	 * a maximum of numTries times.
	 */
	private void broadcast(KVMessage message, int numTries) {
		//connect to each server and send them the metadata
		logger.info("Broadcasting "+message.getMsg());
		ArrayList<Client> clients = getActiveServers(numTries);
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
		
		// Launch the server
		String jarPath = new File(System.getProperty("user.dir"), "ms2-server.jar").toString();
		String launchCmd = "java -jar "+jarPath+" "+server.port+" "+cacheSize+" "+replacementStrategy+" "+server.id; 
		String sshCmd = "ssh -n "+server.ipAddress+" nohup "+launchCmd;
		
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
		logger.info("Killing server "+server.ipAddress+" "+server.port);
		if (p != null){
			p.destroy();
		}
		allProcesses.set(id, null);
		
		String killCmd = "ssh -n "+ server.ipAddress +" nohup fuser -k " + server.port + "/tcp";
		try {
			Process killing_p = Runtime.getRuntime().exec(killCmd);
			//killing_p.destroy();
		} catch (IOException e) {
			System.out.println(e.getMessage()); 
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
	public boolean writeMetadata() {
		// Block read if metadata is locked
		while(CheckIfLockFileExist()){}
		// Lock the File by creating the lock file
		CreateLockFile();
		try {
			PrintWriter writer = new PrintWriter(metadataFile, "UTF-8");
			writer.println(metadata.toString());
			writer.close();
			// Release Lock
			RemoveLockFile();
			return true;
		}
		catch (Exception e) {
			logger.warn("Could not write metadata to file");
			// Release Lock if exception
			RemoveLockFile();
			return false;
		}
	}
	
	/**
	 * Write metadata to metadata file.
	 */
	public String readMetadata() {
		// Block read if metadata is locked
		while(CheckIfLockFileExist()){}
		// Lock the File by creating the lock file
		CreateLockFile();
		try {
			BufferedReader FileReader = new BufferedReader(new FileReader(metadataFile));
			String data = FileReader.readLine();
			metadata = new HashRing(data);
			FileReader.close();
			// Release Lock
			RemoveLockFile();
			return metadata.toString();
		}
		catch (Exception e) {
			logger.warn("Could not read metadata from file");
			// Release Lock if exception
			RemoveLockFile();
			return null;
		}
	}
	
	public void printState() {
		System.out.println("\nStorage service current state");
		System.out.println("==========================================");
		System.out.println("Status: "+(status==ServerStatus.STOPPED ? "STOPPED" : "ACTIVE"));
		System.out.println(totalNumNodes+" servers in the system");
		for (int id=0; id<totalNumNodes; id++){
			Server server = allServers.get(id);
			System.out.println("Server "+server.toString()+"\t\t"+(metadata.contains(server) ? "online" : "offline"));
		}
		System.out.println("");
	}
}