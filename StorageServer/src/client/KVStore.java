package client;


import java.io.IOException;
import java.net.UnknownHostException;

import common.messages.KVMessage;
import common.messages.MessageType;

import client.Client;
import client.TextMessage;
import client.KVCommInterface.SocketStatus;

import app_kvClient.KVClient;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	private KVClient kvclient = null;
	private KVMessage response;
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 * @param kvclient KVClient object constructing the KVStore (give pointer "this")
	 */
	public KVStore(String address, int port, KVClient kvclient) {
		this.address = address;
		this.port = port;
		this.kvclient = kvclient;
	}
	
	/**
	 * Initialize KVStore with address and port of KVServer, without a KVClient.
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		this.kvclient = null;
	}
	
	@Override
	public void connect() 
		throws UnknownHostException, IOException {
		client = new Client(address, port);
		client.addListener(this);
		client.start();
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
		KVMessage request = new MessageType(String.join(" ","put",key,value), false);
		response = null;
		client.sendMessage(request);	
		//Wait for client thread to receive message. When it does it will call handleNewMessage,
		//which puts the message in response.
		//TODO: could this cause a race condition? 
		//TODO: what if client or server dies? Need to terminate if response is not received after
		//a certain amount of time.
		while (response == null){
			;
		}
		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		KVMessage request = new MessageType(String.join(" ","get",key), false);
		response = null;
		client.sendMessage(request);	
		//Wait for client thread to receive message. When it does it will call handleNewMessage,
		//which puts the message in response.
		//TODO: could this cause a race condition? 
		//TODO: what if client or server dies? Need to terminate if response is not received after
		//a certain amount of time.
		while (response == null){
			;
		}
		return response;
	}
	
	public void handleNewMessage(KVMessage msg){
		response = msg;
		//call KVClient.handleNewMessage which prints it
		//TODO: do we really need this? Maybe just have the kvclient print the message it
		//gets from KVStore.put() or KVStore.get()
		if (kvclient != null){
			kvclient.handleNewMessage(msg);
		}
	}
	
	public void handleStatus(SocketStatus status){
		KVMessage msg = null;
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			msg = new MessageType("disconnect DISCONNECTED",true);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			msg = new MessageType("disconnect CONNECTION_LOST",true);
		}
		response = msg;
		
		//Wrapper around KVClient.handle status. No need to do anything
		//if kvclient is null.
		if (kvclient != null){
			kvclient.handleStatus(status);
		}
	}

	
}
