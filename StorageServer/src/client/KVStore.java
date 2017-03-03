package client;


import java.io.IOException;
import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.MessageType;

import client.Client;
import client.KVCommInterface.SocketStatus;

import app_kvClient.KVClient;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
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
		//empty fields not allowed with our communication protocol. Use space instead.
		if (key.equals("")){
			key = " ";
		}
		if (value.equals("")){
			value = " ";
		}
		MessageType request = new MessageType("put"," ",key,value);
		if (request.error != null){
			throw new Exception(request.error);
		}

		// In Client.java
		client.sendMessage(request);
		//Wait for client thread to receive message from server (Client.java function)
		KVMessage response = client.getResponse();
		
		// Should not print in KVStore
		//System.out.println("Received response " + response.getStatus() + "\n");
		
		if (response != null){
			// Log the response and process what to do
			client.logInfo("KVStore: received response  "+response.getMsg());
			if(response.getStatus().equals("SERVER_WRITE_LOCK")) {
				// If write locked then a new server is being added and data is being transferred
				// We block until server is ready to receive (?)
				return get(key);
			} else if(response.getStatus().equals("SERVER_NOT_RESPONSIBLE")) {
				// TODO: get metadata and get data from another server
			} else if(response.getStatus().equals("SERVER_STOPPED")) {
				// TODO: what to do when requested server is stopped
			}
		}
		else{
			client.logInfo("KVStore: no response received");
			System.out.println("KVStore: no response received");
		}
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		//empty fields not allowed with our communication protocol. Use space instead.
		if (key.equals("")){
			key = " ";
		}
		MessageType request = new MessageType("get"," ",key," ");
		if (request.error != null){
			throw new Exception(request.error);
		}

		// In Client.java
		client.sendMessage(request);	
		//Wait for client thread to receive message from server (Client.java function)
		KVMessage response = client.getResponse();
		
		// Should not print in KVStore
		//System.out.println("Received response " + response.getStatus() + "\n");
		
		if (response != null){
			// Log the response and process what to do
			client.logInfo("KVStore: received response  "+response.getMsg());
			if(response.getStatus() == "SERVER_NOT_RESPONSIBLE") {
				// TODO: get metadata and get data from another server
			} else if(response.getStatus() == "SERVER_STOPPED") {
				// TODO: what to do when requested server is stopped
			}
		}
		else{
			client.logInfo("KVStore: no response received");
			System.out.println("KVStore: no response received");
		}
		return response;
	}
	
}
