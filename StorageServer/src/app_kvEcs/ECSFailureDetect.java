package app_kvEcs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.BindException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.*;

import org.apache.log4j.Logger;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;
import client.Client;
import common.HashRing;
import common.HashRing.Server;
import common.messages.KVMessage;

public class ECSFailureDetect extends Thread {

	public boolean m_running;
	private ECS m_ecs;

	/**
	 * Constructs a new HeartBeat object for a given TCP socket.
	 * @throws Exception 
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public ECSFailureDetect(String configFile) throws FileNotFoundException, IOException, Exception {
		this.m_ecs = new ECS(configFile);
		this.m_running = true;
	}

	/**
	 * Runs the thread. 
	 * Loops until ECS is shutdown.
	 */
	public void run() {
		while (isRunning()) {
			// Periodically check whether servers are alive using ECS funtion GetAllServers
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {}
			List<Server> failedServers = detectFailures();
			restoreService(failedServers);
		}
	}
	
	public void stopFailureDetect() {
		this.m_running = false;
	}

	private boolean isRunning() {
		return this.m_running;
	}
	
	/**
	 * Tries to connect to every server in the metadata and returns and list of all
	 * servers which it could not connect to.
	 */
	public List<Server> detectFailures() {
		System.out.println("Checking for server failures");
		HashRing metadata = m_ecs.getMetaData();
		List<Server> activeServers = metadata.getAllServers();
		List<Server> failedServers = new ArrayList<Server>();
		
		for (Server server : activeServers) {
			int triesRemaining = 3;
			boolean success = false;
			
			System.out.println("Checking if online: " + server.toString());
			
			while (triesRemaining-- > 0){
				//try connecting to this server 
				success = false;
				try {
					// Only a connection test, set a low timeout
					Client client = new Client(server.ipAddress, server.port, 1000);
					//wait for "connection successful" response
					KVMessage response = client.getResponse();
					client.closeConnection();
					if (response.getStatus().equals("CONNECT_SUCCESS")) {
						// Make sure it is connect success. Could also receive TIME_OUT
						success = true;
						break;
					}
				} catch (Exception e){}
				//wait a bit before trying to connect again
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}

			if (!success) {
				failedServers.add(server);
			}
		}
		return failedServers;
	}
	
	/**
	 * Tries to reconstruct the service after failedServers servers have failed by transferring 
	 * the data around to maintain the replication invariant defined in milestone 3.
	 */
	public boolean restoreService(List<Server> failedServers, boolean doAdd) {
		boolean success = true;
		for (Server server : failedServers) {
			System.out.println("Server "+server+" appears to have crashed");
			//This server seems to have failed. Handle it by calling ecs.removeNode
			//Note that ecs.removeNode does not need the server to be alive to operate. It
			//just moves around the data to account for the loss. 
			if(!m_ecs.removeNodeReconstruct(server)) {
		        success = false;
		        System.out.println("Remove node in Failure Detection FAILED!");
		    }
			//success = success && m_ecs.removeNodeReconstruct(server);	
			//replace the dead server
			if (doAdd) {
				success = success && m_ecs.addRandomNode(ECS.cacheSize, ECS.replacementStrategy);
			}
			//Note: removeNode and addNode will update everyone's metadata			
		}
		return success;
	}

	public boolean restoreService(List<Server> failedServers) {
		return restoreService(failedServers, true);
	}
}
