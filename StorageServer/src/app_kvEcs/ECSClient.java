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
	private ECS ecs;
	
	ECSClient(String configFile){
		this.ecs = new ECS(configFile);
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
			ecs.initService();
			break;
		case "start":
			ecs.start();
			break;
		case "stop":
			ecs.stop();
			break;
		case "shutDown":
			stopECSClient = true;
			ecs.shutDown();
			break;
		case "addNode":
			try{
				int cacheSize = Integer.parseInt(tokens[1]);
				ecs.addNode(cacheSize, tokens[2]);
			}
			catch (NumberFormatException e){
				printError("Cache size must be an integer");
			}
			break;
		case "removeNode":
			try{
				int index = Integer.parseInt(tokens[1]);
				ecs.removeNode(index);
			}
			catch (NumberFormatException e){
				printError("Index must be an integer");
			}
			break;
		}
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
