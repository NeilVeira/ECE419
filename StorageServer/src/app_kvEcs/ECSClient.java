package app_kvEcs;

import org.apache.zookeeper.ZooKeeper;
import java.io.*;

public class ECSClient extends Thread {
	private File configFile;
	private ZooKeeper zookeeper;
	
	ECSClient(String configFile){
		this.configFile = new File(configFile);
	}
	
	/**
	 * Reads and parses the ecs config file, launches the servers, and initializes
	 * the zookeeper object. 
	 * @throws FileNotFoundException
	 */
	public boolean initService() {
		String connectString = ""; //comma-separated list of "IP address:port" pairs for zookeeper
		String currentLine;
		
		try{
			BufferedReader FileReader = new BufferedReader(new FileReader(this.configFile));
			while ((currentLine = FileReader.readLine()) != null) {
				String[] tokens = currentLine.split(" ");
				connectString += tokens[1]+":"+tokens[2]+",";
				//TODO: do we launch the servers from here?
			}
			
			//TODO: initialize zookeeper
			
			return true;
		}
		catch (Exception e){
			System.out.println("Error encountered in initService!");
			System.out.println(e.getMessage());
			return false;
		}
		
	}
	
	/**
	 * Entry point to run the ECS
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1){
			System.out.println("Error! Invalid number of arguments!");
			System.out.println("Usage: Server <string config file>");
		}
		else{
			ECSClient ecsClient = new ECSClient(args[0]);
			ecsClient.initService();
		}
	}
}
