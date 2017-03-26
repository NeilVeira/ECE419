package app_kvEcs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.BindException;
import java.net.SocketException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.net.SocketTimeoutException;

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
	 * @param clientSocket the Socket object for the client connection.
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
			detectFailures();
		}
	}
	
	public void stopFailureDetect() {
		this.m_running = false;
	}

	private boolean isRunning() {
		return this.m_running;
	}
	
	public void detectFailures() {
		System.out.println("Checking for server failures");
		HashRing metadata = m_ecs.getMetaData();
		List<Server> activeServers = metadata.getAllServers();
		
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
					if (response != null) {
						success = true;
						break;
					}
				} catch (SocketTimeoutException e) {} catch (Exception e){}
				//wait a bit before trying to connect again
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {}
			}

			if (!success) {
				System.out.println("Server "+server+" appears to have crashed");
				//This server seems to have failed. Handle it by calling ecs.removeNode
				//Note that ecs.removeNode does not need the server to be alive to operate. It
				//just moves around the data to account for the loss. 
				m_ecs.removeNode(server.id);	
				//replace the dead server
				m_ecs.addRandomNode(ECS.cacheSize, ECS.replacementStrategy);
				//Note: removeNode and addNode will update everyone's metadata
			}
		}
	}

}
