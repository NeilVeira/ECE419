package client;

import java.io.IOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import common.messages.KVMessage;
import common.messages.MessageType;
import common.HashRing;
import common.HashRing.Server;

import client.Client;
import client.KVCommInterface.SocketStatus;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	private HashRing metadata;
	private Logger logger = Logger.getRootLogger();
	private boolean connected = false;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		metadata = new HashRing();
		//add the given server to the metadata. This will be KVStore's first try when doing a request.
		metadata.addServer(new Server(address,port));
	}
	
	@Override
	public boolean connect() 
		throws UnknownHostException, IOException, ConnectException{
		try{
		client = new Client(address, port);
		} catch (ConnectException e) {
			System.out.println("Connection refused!");
			return false;
		}
		logger.info("Client trying to connect...");
		client.addListener(this);
		//client.start();
		//wait for "connection successful" response
		KVMessage response = client.getResponse();
		if (response != null){
			logger.info("KVStore: received response "+response.getMsg());
			connected = true;
			return true;
		}
		return false;
	}
	
	public boolean isConnected() {
		return connected;
	}

	@Override
	public void disconnect() {
		if(client != null) {
			client.closeConnection();
			client = null;
		}
	}
	

	@Override
	public KVMessage put(String key, String value) throws Exception {		
		MessageType request = new MessageType("put","",key,value);
		if (request.error != null){
			throw new Exception(request.error);
		}		
		connectToResponsible(key);
		return sendRequest(request);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		MessageType request = new MessageType("get","",key,"");
		if (request.error != null){
			throw new Exception(request.error);
		}
		boolean success = connectToResponsible(key);
		return sendRequest(request);
	}
	
	/**
	 * Check which server is responsible for the given key from the cached metadata
	 * and try connecting to it. If unable to connect, try to connect to any server.
	 */
	private boolean connectToResponsible(String key) {
		Server responsible = metadata.getResponsible(key);
		if (responsible == null) {
			return false;
		}
		logger.debug("Trying to connect to responsible server "+responsible.toString());
		disconnect();
		
		try {
			this.address = responsible.ipAddress;
			this.port = responsible.port;
			boolean ok = connect();
			if (!ok){
				return connectToAnyServer();
			}
		} catch (Exception e) {
			logger.debug("Unable to connect to responsible server "+responsible.toString());
			return connectToAnyServer();
		}
		
		return true;
	}
	
	/**
	 * Try to send the request to the server. 
	 * If the server replies with SERVER_WRITE_LOCK or SERVER_STOPPED, wait and then try again. 
	 * If the server replies with SERVER_NOT_RESPONSIBLE, update the metadata, determine what server
	 * should be responsible, connect to it, and try again. 
	 */
	private KVMessage sendRequest(KVMessage request) {
		KVMessage response = null;
		int writeLockCount = 20; //maximum number of times to retry if we get a SERVER_WRITE_LOCK response
		
		do {
			logger.info("KVStore: sending request "+request.getMsg());
			try {
				if (client == null) {
					connectToAnyServer();
				}
				client.sendMessage(request);
			}
			catch (IOException e) {
				boolean success = connectToAnyServer();
				if (!success) {
					return null;
				}
			}
			
			//Wait for client thread to receive message from server (Client.java function)
			//TODO: timeout if no response is received
			response = client.getResponse();
			logger.info("KVStore: received response  "+response.getMsg());
			
			if (response.getStatus().equals("SERVER_STOPPED")){
				//The entire system is disabled for an indefinite amount of time, so there's 
				//no point waiting and trying again. Give the user the stopped message.
				return new MessageType(request.getHeader(), "SERVER_STOPPED", request.getKey(), request.getValue());
			}
			else if (response.getStatus().equals("SERVER_WRITE_LOCK")){
				// If write locked then a new server is being added and data is being transferred
				// We block until server is ready to receive

				logger.info("Server is temporarily locked for writing. Waiting and retrying");
				// We try maximum of 20 times, each time with 500ms delay, to reach a server with write_lock
				writeLockCount--;
				try {
					Thread.sleep(500);
				} catch (InterruptedException e){}
				
			}
			else if (response.getStatus().equals("SERVER_NOT_RESPONSIBLE")) {
				// get update metadata and determine responsible server
				String mdata = response.getValue(); 
				this.metadata = new HashRing(mdata);
				HashRing.Server responsibleServer = metadata.getResponsible(request.getKey());

				logger.info("Received SERVER_NOT_RESPONSIBLE. Connecting to server "+responsibleServer.toString());
				//System.out.println("Received SERVER_NOT_RESPONSIBLE. Connecting to server "+responsibleServer.toString());

				//disconnect from the current server and try to connect to the new one
				disconnect();
				this.address = responsibleServer.ipAddress;
				this.port = responsibleServer.port;
				try {
					connect();
				} 
				catch(Exception e) {
					//try to connect to any other server in the metadata
					boolean success = connectToAnyServer();
					if (!success) {
						return null;
					}
				}
				return sendRequest(request);
			}
			else {
				break;
			}
			
		} while (writeLockCount > 0);
		
		if (response == null){
			logger.info("KVStore: no response received");
		}				
		else if (response.getStatus().equals("SERVER_WRITE_LOCK")){
			logger.error("Timed out retrying on server with write lock!");
			// Returns a PUT_ERROR to the KVClient, since there is no other suitable status code
			return new MessageType(response.getHeader(), "PUT_ERROR", response.getKey(), response.getValue());
		}
		return response;
	}
	
	/**
	 * Run through all the known servers in the metadata, trying to connect to any of them.
	 * Returns true if successful.
	 */
	private boolean connectToAnyServer() {
		List<Server> allServers = metadata.getAllServers();
		for (Server server : allServers) {
			logger.debug("Trying to connect to server "+server.toString());
			this.address = server.ipAddress;
			this.port = server.port;
			try {
				boolean success = connect();
				if (success) {
					return true;
				}
			} catch(Exception ex) {
				
			}
		}
		logger.error("KVStore: Unable to connect to any server in the system.");
		return false;
	}
}
