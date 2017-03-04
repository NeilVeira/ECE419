package client;

import java.io.IOException;

import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.MessageType;
import common.HashRing;

import client.Client;
import client.KVCommInterface.SocketStatus;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	private HashRing metadata;
	
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
		metadata.addServer(metadata.new Server(address,port));
	}
	
	@Override
	public void connect() 
		throws UnknownHostException, IOException{
		client = new Client(address, port);
		client.logInfo("Client trying to connect...");
		client.addListener(this);
		//client.start();
		//wait for "connection successful" response
		KVMessage response = client.getResponse();
		if (response != null){
			client.logInfo("KVStore: received response "+response.getMsg());
		}
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
		return sendRequest(request);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		MessageType request = new MessageType("get","",key,"");
		if (request.error != null){
			throw new Exception(request.error);
		}
		return sendRequest(request);
	}
	
	/**
	 * Try to send the request to the server. 
	 * If the server replies with SERVER_WRITE_LOCK or SERVER_STOPPED, wait and then try again. 
	 * If the server replies with SERVER_NOT_RESPONSIBLE, update the metadata, determine what server
	 * should be responsible, connect to it, and try again. 
	 */
	private KVMessage sendRequest(KVMessage request) {
		KVMessage response = null;
		do {
			client.logInfo("KVStore: sending request "+request.getMsg());
			try {
				client.sendMessage(request);
			}
			catch (IOException e) {
				client.logError("Unable to send request "+request.getMsg());
				return null;
			}
			
			//Wait for client thread to receive message from server (Client.java function)
			//TODO: timeout if no response is received
			response = client.getResponse();
			client.logInfo("KVStore: received response  "+response.getMsg());
			
			if (response.getStatus().equals("SERVER_WRITE_LOCK") || response.getStatus().equals("SERVER_STOPPED")){
				// If write locked then a new server is being added and data is being transferred
				// We block until server is ready to receive (?)
				//TODO: block for some time before trying again
			}
			else if (response.getStatus().equals("SERVER_NOT_RESPONSIBLE")){
				// get update metadata and determine responsible server
				String mdata = response.getValue(); 
				this.metadata = new Metadata(mdata);
				Metadata.Server responsibleServer = metadata.getResponsible(request.getKey());
				//disconnect from the current server and try to connect to the new one
				disconnect();
				this.address = responsibleServer.ipAddress;
				this.port = responsibleServer.port;
				try {
					connect();
				} catch(Exception e) {
					//TODO: try to connect to any other server in the metadata
					client.logError("KVStore: Unable to connect to new server "+this.address+" on port "+this.port);
					return null;
				}
			}
			else {
				break;
			}
			
		} while (true);
		
		if (response == null){
			client.logInfo("KVStore: no response received");
		}
		return response;
	}
}
