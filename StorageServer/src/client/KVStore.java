package client;


import java.io.IOException;
import java.net.UnknownHostException;

//import common.messages.KVMessage;
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
		throws UnknownHostException, IOException {
		client = new Client(address, port);
		client.addListener(this);
		//client.start();
		//wait for "connection successful" response
		MessageType response = client.getResponse();
		if (response != null){
			System.out.println("response: "+response.getMsg());
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
	public MessageType put(String key, String value) throws Exception {
		MessageType request = new MessageType("put " + key + " " + value, false);
		System.out.println("request: " + request.getMsg());
		client.sendMessage(request);
		//Wait for client thread to receive message from server
		MessageType response = client.getResponse();
		//TODO: use logging instead of printing to console
		if (response != null){
			System.out.println("response: "+response.getMsg());
		}
		else{
			System.out.println("no response received");
		}
		return response;
	}

	@Override
	public MessageType get(String key) throws Exception {
		MessageType request = new MessageType("get "+key, false);
		System.out.println("request: " + request.getMsg());
		//response = null;
		client.sendMessage(request);	
		//Wait for client thread to receive message from server
		MessageType response = client.getResponse();
		//TODO: use logging instead of printing to console
		if (response != null){
			System.out.println("response: "+response.getMsg());
		}
		else{
			System.out.println("no response received");
		}
		return response;
	}
	
	/*public void handleNewMessage(MessageType msg){
		System.out.println("got response "+msg.getMsg());
		response = msg;
	}
	
	public void handleStatus(SocketStatus status){
		MessageType msg = null;
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			msg = new MessageType("disconnect DISCONNECTED",true);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			msg = new MessageType("disconnect CONNECTION_LOST",true);
		}
		
		response = msg;
	}*/
	

	
}
