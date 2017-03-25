package app_kvEcs;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.BindException;
import java.net.SocketException;

import org.apache.log4j.Logger;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;

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
	public ECSFailureDetect(Socket clientSocket) throws FileNotFoundException, IOException, Exception {
		this.m_ecs = new ECS("ecs.config");
	}

	/**
	 * Runs the thread. 
	 * Loops until ECS is shutdown.
	 */
	public void run() {
		while (isRunning()) {
			// Periodically check whether servers are alive using ECS funtion GetAllServers
		}
		
	}

	private boolean isRunning() {
		return this.m_running;
	}

}
