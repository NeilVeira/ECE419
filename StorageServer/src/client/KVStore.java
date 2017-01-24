package client;


import java.io.IOException;
import java.net.UnknownHostException;

import common.messages.KVMessage;

import client.Client;
import client.TextMessage;
import client.KVCommInterface.SocketStatus;

import app_kvClient.KVClient;

public class KVStore implements KVCommInterface {
	private String address;
	private int port;
	private Client client = null;
	private KVClient kvclient = null;
	
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void handleNewMessage(TextMessage msg){
		//Wrapper around KVClient.handle status. No need to do anything
		//if kvclient is null.
		if (kvclient != null){
			kvclient.handleNewMessage(msg);
		}
	}
	
	public void handleStatus(SocketStatus status){
		//Wrapper around KVClient.handle status. No need to do anything
		//if kvclient is null.
		if (kvclient != null){
			kvclient.handleStatus(status);
		}
	}

	
}
