package client;

import java.io.IOException;

import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.MessageType;
import common.Metadata;

import client.Client;
import client.KVCommInterface.SocketStatus;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	private Metadata metadata;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		metadata = new Metadata();
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
		/*//empty fields not allowed with our communication protocol. Use space instead.
		if (key.equals("")){
			key = " ";
		}
		if (value.equals("")){
			value = " ";
		}*/
		//empty fields should be ok now
		
		MessageType request = new MessageType("put"," ",key,value);
		if (request.error != null){
			throw new Exception(request.error);
		}
		
		client.sendMessage(request);
		//Wait for client thread to receive message from server
		KVMessage response = client.getResponse();
		
		if (response != null){
			client.logInfo("KVStore: received response  "+response.getMsg());
			System.out.println("KVStore: received response  "+response.getMsg());
		}
		else{
			client.logInfo("KVStore: no response received");
			System.out.println("KVStore: no response received");
		}
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		/*//empty fields not allowed with our communication protocol. Use space instead.
		if (key.equals("")){
			key = " ";
		}
		if (request.error != null){
			throw new Exception(request.error);
		}*/
		//empty fields should be ok now
		
		MessageType request = new MessageType("get","",key,"");
		//System.out.println("request: " + request.getMsg());
		client.sendMessage(request);	
		//Wait for client thread to receive message from server
		KVMessage response = client.getResponse();
		if (response != null){
			client.logInfo("KVStore: received response  "+response.getMsg());
			System.out.println("KVStore: received response  "+response.getMsg());
		}
		else{
			client.logInfo("KVStore: no response received");
			System.out.println("KVStore: no response received");
		}
		return response;
	}
	
}
