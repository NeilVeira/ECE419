package app_kvEcs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.zookeeper.ZooKeeper;

public class ECS {
	private File configFile;
	private ZooKeeper zookeeper;
	
	public ECS(String configFile){
		this.configFile = new File(configFile);
	}
	
	/**
	 * Reads and parses the ecs config file, launches the servers, and initializes
	 * the zookeeper object. 
	 */
	public boolean initService() {
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