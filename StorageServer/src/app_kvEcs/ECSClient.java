package app_kvEcs;

import org.apache.log4j.Level;
import logger.LogSetup;
import org.apache.log4j.Logger;

import org.apache.zookeeper.ZooKeeper;
import java.io.*;

/**
 * Class to implement the command line interface for the ECS, similar to KVClient
 */
public class ECSClient {
	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "StorageServiceECS> ";
	private BufferedReader stdin;
	private boolean stopECSClient;
	
	private File configFile;
	private ZooKeeper zookeeper;
	
	ECSClient(String configFile){
		this.configFile = new File(configFile);
		stopECSClient = false;
	}
	
	public void run() {
		while (!stopECSClient){
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);
			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stopECSClient = true;
				printError("An error occurred - Application terminated ");
			}
		}
	}
	
	/**
	 * Handle string command from command line
	 */
	public void handleCommand(String cmdLine) {
		//TODO: logging
		String[] tokens = cmdLine.split("\\s+");
		if (tokens.length == 0){
			//ignore empty command
			return;
		}
		
		//Check that the command has the correct number of arguments
		int expectedNumArgs = 0;
		switch(tokens[0]){
		case "initService":
		case "start":
		case "stop":
		case "shutDown":
			expectedNumArgs = 0;
			break;
		case "addNode":
			expectedNumArgs = 2;
			break;
		case "removeNode":
			expectedNumArgs = 1;
			break;
		default:
			printError("Unknown command "+tokens[0]);
			return;			
		}
		
		if (tokens.length != expectedNumArgs+1){
			printError("Incorrect number of arguments for command "+tokens[0]);
			return;
		}
		
		//call corresponding ECS method
		switch (tokens[0]){
		case "initService":
			initService();
			break;
		case "start":
			start();
			break;
		case "stop":
			stop();
			break;
		case "shutDown":
			shutDown();
			break;
		case "addNode":
			try{
				int cacheSize = Integer.parseInt(tokens[1]);
				addNode(cacheSize, tokens[2]);
			}
			catch (NumberFormatException e){
				printError("Cache size must be an integer");
			}
			break;
		case "removeNode":
			try{
				int index = Integer.parseInt(tokens[1]);
				removeNode(index);
			}
			catch (NumberFormatException e){
				printError("Index must be an integer");
			}
			break;
		}
	}
	
	/**
	 * Reads and parses the ecs config file, launches the servers, and initializes
	 * the zookeeper object. 
	 */
	private boolean initService() {
		System.out.println("Initializing service");
		String connectString = ""; //comma-separated list of "IP address:port" pairs for zookeeper
		String currentLine;
		
		try{
			BufferedReader FileReader = new BufferedReader(new FileReader(this.configFile));
			while ((currentLine = FileReader.readLine()) != null) {
				String[] tokens = currentLine.split(" ");
				connectString += tokens[1]+":"+tokens[2]+",";
				//TODO: store this somewhere for start() method to launch them
			}
			
			//TODO: initialize zookeeper
			
			return true;
		}
		catch (FileNotFoundException e) {
			System.out.println("Could not find config file "+this.configFile);
			return false;
		}
		catch (Exception e){
			System.out.println("Error encountered in initService!");
			System.out.println(e.getMessage());
			return false;
		}
	}
	
	/**
	 * Launch the storage servers chosen by initService
	 */
	private void start() {
		//TODO
		System.out.println("Starting");
	}
	
	/**
	 * Stops all running servers in the service
	 */
	private void stop() {
		//TODO
		System.out.println("Stopping");
	}
	
	/**
	 * Stops all servers and shuts down the ECS client
	 */
	private void shutDown() {
		stop();
		stopECSClient = true;
	}
	
	/**
	 * Creates a new server with the given cache size and replacement strategy
	 * and adds it to the service
	 */
	private void addNode(int cacheSize, String replacementStragey) {
		//TODO
		System.out.println("Adding node "+cacheSize+" "+replacementStragey);
	}
	
	/**
	 * Remove the node with the given index
	 */
	private void removeNode(int index) {
		//TODO
		System.out.println("Removing node "+index);
	}
	
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
	}
	
	
	/**
	 * Entry point to run the ECS
	 * @param args
	 */
	public static void main(String[] args) {
		try{
			if (args.length != 1){
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <string config file>");
			}
			else{
				new LogSetup("logs/ecs.log", Level.WARN);
				ECSClient ecsClient = new ECSClient(args[0]);
				ecsClient.run();
			}
		} 
		catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}
}
