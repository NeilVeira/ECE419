/***
 * These tests test the functionality of the ECS, KVServer, and KVStore classes as they 
 * interact together. They are long running tests and are run separately because they involve 
 * initializing and shutting down the ECS and lots of killing and launching of servers which 
 * takes a long time due to ssh. 
 */

package testing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ConnectException;
import java.io.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.*;

import junit.framework.TestCase;

import org.apache.log4j.Level;
import org.junit.Test;

import app_kvClient.KVClient;

import app_kvEcs.ECS;
import app_kvEcs.ECSClient;
import app_kvEcs.ECSFailureDetect;

import app_kvServer.ClientConnection;
import app_kvServer.KVServer;

import client.Client;
import client.KVCommInterface;
import client.KVStore;

import common.HashRing;
import common.HashRing.Server;

import common.messages.KVMessage;
import common.messages.KVAdminMessage;
import common.messages.MessageType;

import logger.LogSetup;

public class IntegrationTest extends TestCase {
	private ECS testECSInstance;
	private List<Server> allServers;
	private ECSFailureDetect testFailureDetect;
	
	static {
		try {
			new LogSetup("logs/testing/test.log", Level.WARN);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void setUp() {
		try {
			// Start ECS by creating an ECS instance and manually running its functions rather than using the ECS client
			System.out.println("Setting Up");
			testECSInstance = new app_kvEcs.ECS("ecstest.config");
			allServers = testECSInstance.getAllServers();
			testECSInstance.clearMetaData();
			testECSInstance.writeMetadata();
			testFailureDetect = new ECSFailureDetect("ecstest.config");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/***
	 * Resets the entire system to a blank state by
	 *      Shutting down the ECS and making sure all servers have died
	 * 		Clearing the ECS metadata file
	 * 		Deleting all servers' storage files
	 */
	public void tearDown() {
		// Reset the metaData and overWrite
		System.out.println("Cleaning Up");
		testECSInstance.clearMetaData();
		boolean success = testECSInstance.writeMetadata();
		assertEquals(success, true);
		testECSInstance.shutDown();

		try {
			//delete all storage files
			for (Server server : allServers) {
				String fileName = "~/storage_"+server.id+".txt";
				String cmd = "ssh -n "+server.ipAddress+" nohup rm -f "+fileName;
				//System.out.println("Running "+cmd);
				Runtime.getRuntime().exec(cmd);
			}
			//wait a bit for everything to finish
			TimeUnit.SECONDS.sleep(10);
		} catch (Exception e) {}
		
		//Make sure all servers are shutdown before proceeding.
		//Note that this is only critical in testing because we're about to launch another ECS a second
		//later (so ECS.shutdown doesn't need to worry about this).
		for (Server server : allServers) {
			boolean ok = serverShutDown(server,10);
			if (!ok){
				System.out.println("ECS shutdown did not kill server "+server.toString()+". Forcing kill again.");
				//try forcing shutdown again
				killServer(server);
				ok = serverShutDown(server,10);
				if (!ok) {
					System.out.println("WARNING: Server "+server.toString()+" does not appear to be shutting down");
				}
			}
		}
	}
	
	/**
	 * Make sure that the given server is shut down and cannot be connected to.
	 * Try tries times, blocking for 1 second between each try.
	 */
	public boolean serverShutDown(Server server, int tries) {
		for (int i=0; i<tries; i++) {
			try {
				new Client(server.ipAddress, server.port);
				TimeUnit.SECONDS.sleep(1);
			} catch (IOException e){
				//failed to connect
				return true;
			}
			catch (InterruptedException e) {}
		}
		return false;
	}
	
	public boolean canConnect(Server server) {
		try {
			new Client(server.ipAddress, server.port);
			return true;
		} catch (IOException e){
			return false;
		}
	}
	
	/**
	 * Run the ssh command to kill a server
	 */
	public void killServer(Server server) {
		String killCmd = "ssh -n "+ server.ipAddress +" nohup fuser -k " + server.port + "/tcp";
		try {
			Runtime.getRuntime().exec(killCmd);
		} catch (IOException e) {
			System.out.println(e.getMessage()); 
		}
	}
	
	/**
	 * Does a put(x,x) operation for all x in the range low..high-1
	 * @param server: A server to connect to
	 */
	public void populateStorage(Server server, int low, int high) 
		throws ConnectException, IOException, Exception {
		System.out.println("Putting data");
		KVStore kvstore = new KVStore(server.ipAddress, server.port);
		kvstore.connect();
		for (int i=low; i<high; i++){
			KVMessage response = kvstore.put(String.valueOf(i),  String.valueOf(i));
			assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS"));
		}
	}
	
	/**
	 * Sends a get message to the given server for the key and asserts that it responds 
	 * with a GET_SUCCESS message containing the correct value.
	 * This function is useful for bypassing the KVStore because we want to see if a server replies with
	 * SERVER_NOT_RESPONSIBLE or GET_ERROR (KVStore would hide those replies and try with a different server)
	 */
	public void queryServer(Server server, String key, String value) 
		throws ConnectException, IOException {
		Client client = new Client(server.ipAddress, server.port);
		client.getResponse(); //connect success
		client.sendMessage(new MessageType("get","",key,""));
		KVMessage response = client.getResponse();
		assertEquals("GET_SUCCESS",response.getStatus());
		assertEquals(value, response.getValue());
	}
	
	/**
	 * Similar to the above function but used when we already have a client connected 
	 * so we don't have to reconnect.
	 */
	public void queryServer(Client client, String key, String value) 
		throws ConnectException, IOException {
		client.sendMessage(new MessageType("get","",key,""));
		KVMessage response = client.getResponse();
		assertEquals("GET_SUCCESS",response.getStatus());
		assertEquals(value, response.getValue());		
	}
	
	/**
	 * Test whether the ECS can properly start a server cluster that allows client connections and shutdown properly
	 */
	@Test
	public void testSetupSanityAndShutDownServers() {
		// Declare Variables for easy modification later on
		// ECS Server Variables
		int InitNumServers = 5;
		int ServerCacheSize = 100;
		String ServerCacheStrategy = "FIFO";
		
		
		// Use our ecstest.config file with ports on the 60000 range
		try {
			// Run ECS initService to start the servers
			System.out.println("initializing Servers");
			testECSInstance.initService(InitNumServers, ServerCacheSize, ServerCacheStrategy);
			// Find out how many Servers are running on the SSH host by getting the Hash Ring from the ECS class
			long SleepDelay = 5;
			// Wait 3 seconds for Servers to start
			TimeUnit.SECONDS.sleep(SleepDelay);
			System.out.println("Finished Waiting Getting Metadata file");
			common.HashRing testECSMetaData = testECSInstance.getMetaData();
			System.out.println("Finished Getting Metadata file");
			// Test that the amount of servers in the metaData is equal to our initialization parameters. (init is successful)
			assertEquals(InitNumServers, testECSMetaData.getAllServers().size());
			System.out.println("initialized all Servers with correct number");
			
			// Test that our Client can at least Connect to the Servers that are up
			// Create a Custom KVStore to directly invoke functions and bypass KVClient
			int counter = 0;
			List<Server> testServerList = new LinkedList<Server>();
			testServerList = testECSMetaData.getAllServers();
			// Get first element of list of servers
			common.HashRing.Server testServer = testServerList.get(counter);
			while (counter < InitNumServers) {
				// Create a Custom KVStore to directly invoke functions and bypass KVClient
				String ServerAddress = testServer.ipAddress;
				int ServerPort = testServer.port;
				client.KVStore testKVStoreInstance = new client.KVStore(ServerAddress,ServerPort);
				System.out.println("Connecting Client to " + ServerAddress + " at Port " + ServerPort);
				boolean Success = testKVStoreInstance.connect();
				assertEquals(true, Success);
				System.out.println(" Success! Connected to " + ServerAddress + " at Port " + ServerPort);
				
				// Update the Server to the next in the list after the counter
				counter = counter + 1;
				if (InitNumServers - 1 >= counter) {
					testServer = testServerList.get(counter);	
				}
			}
			
			// Next ShutDown the Servers with ECS and see if Clients can still connect to the servers or not
			System.out.println("Shutting down all servers");
			testECSInstance.shutDown();
			System.out.println("Shutted Down waiting 15 seconds for threads to be killed");
			SleepDelay = 15;
			// Wait 10 seconds for Servers to shut down
			TimeUnit.SECONDS.sleep(SleepDelay);
			
			// Test that our Client can no longer Connect to the Servers that were up
			// Create a Custom KVStore to directly invoke functions and bypass KVClient
			counter = 0;
			// Get first element of list of servers
			testServer = testServerList.get(counter);
			while (counter < InitNumServers) {
				// Create a Custom KVStore to directly invoke functions and bypass KVClient
				String ServerAddress = testServer.ipAddress;
				int ServerPort = testServer.port;
				client.KVStore testKVStoreInstance = new client.KVStore(ServerAddress,ServerPort);
				System.out.println("Connecting Client to " + ServerAddress + " at Port " + ServerPort);
				boolean Success = testKVStoreInstance.connect();
				assertEquals(false, Success);
				System.out.println(" Success! " + ServerAddress + " at Port " + ServerPort + " Was shut down");
				
				// Update the Server to the next in the list after the counter
				counter = counter + 1;
				if (InitNumServers - 1 >= counter) {
					testServer = testServerList.get(counter);	
				}
			}
			System.out.println(" Test Finished and Passed");
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Tests that all servers reply SERVER_STOPPED to put and get requests when the ECS is initialized
	 */
	@Test
	public void testServerStopped() {
		System.out.println("Starting testServerStopped");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			
			for (Server server : servers){
				System.out.println("Connecting to server "+server.toString());
				Client client = new Client(server.ipAddress, server.port);
				KVMessage response = client.getResponse();
				
				//make sure get responds server stopped
				client.sendMessage(new MessageType("get","","0",""));
				response = client.getResponse();
				assertEquals("SERVER_STOPPED", response.getStatus());
				
				//make sure put responds server stopped
				client.sendMessage(new MessageType("put","","0","0"));
				response = client.getResponse();
				assertEquals("SERVER_STOPPED", response.getStatus());
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that the ECS can start and stop servers and that they only reply "SERVER_STOPPED" when stopped
	 */
	@Test
	public void testStartAndStopServers() {
		System.out.println("Starting testStartAndStopServers");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(2, 10, "FIFO");
			testECSInstance.start();
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			
			for (Server server : servers){
				System.out.println("Connecting to server "+server.toString());
				KVStore client = new KVStore(server.ipAddress, server.port);
				//make sure it is not stopped
				System.out.println("Doing put");
				KVMessage response = client.put("0", "0");
				assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
			}
			
			//make sure all servers are now stopped
			testECSInstance.stop();
			for (Server server : servers){
				System.out.println("Connecting to server "+server.toString());
				KVStore client = new KVStore(server.ipAddress, server.port);
				//make sure it is stopped
				KVMessage response = client.put("0", "0");
				assertEquals("SERVER_STOPPED",response.getStatus());
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that removeNode correctly shuts down the server 
	 */
	@Test
	public void testRemoveNode() {
		System.out.println("Starting testRemoveNode");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			Server server = testECSInstance.getMetaData().getAllServers().get(0);
			
			//make sure we can connect now
			try {
				new Client(server.ipAddress, server.port);
			} catch (IOException e){
				assertTrue(false);
			}
			
			testECSInstance.removeNode(server.id);
			
			//make sure that we can't connect now	
			//need to try a few times and wait a bit for the server to actually be killed
			boolean shutDown = serverShutDown(server, 5);
			assertTrue(shutDown);			
			
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that after we run ECS.addNode we are able to connect to the new server
	 */
	@Test
	public void testAddNode() {
		System.out.println("Starting testAddNode");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 100, "FIFO");
			Server server = testECSInstance.getMetaData().getAllServers().get(0);
			testECSInstance.removeNode(server.id);
			
			Server newServer = allServers.get(5);
			testECSInstance.addNode(newServer.id, 1000, "LRU");
			//make sure we can connect to the new server
			boolean connect = canConnect(newServer);
			assertTrue(connect);
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that ECS starts a multi-server system and that the client can do gets and puts
	 * This is a simple case where the same client does the get & put.
	 */
	@Test
	public void testPutAndGetMultipleServersSimple() {
		System.out.println("Starting testPutAndGetMultipleServersSimple");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				testECSInstance.removeNode(firstId);
			}
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			//connect to the first server
			Server connectTo = servers.get(0);
			System.out.println("Connect to server "+connectTo);
			KVStore client = new KVStore(connectTo.ipAddress, connectTo.port);
			client.connect();			
			
			//put the value
			KVMessage response = client.put("3", "3");
			assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
			
			//read it back and make sure it's correct
			response = client.get("3");
			assertEquals("GET_SUCCESS",response.getStatus());
			assertEquals("3",response.getValue());
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that client receives a "SERVER_NOT_RESPONSIBLE" message when doing a put to the wrong server
	 */
	@Test
	public void testPutNotResponsible() {
		System.out.println("Starting testPutNotResponsible");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			System.out.println("Done initializing ECS");
			
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				System.out.println("removing node "+firstId);
				testECSInstance.removeNode(firstId);
			}
			System.out.println("adding nodes ");
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			testECSInstance.addNode(6,  500, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			Server responsible = testECSInstance.getMetaData().getResponsible("0");
			//find another server which is different from the responsible one
			Server notResponsible = null;
			for (Server s : servers) {
				if (s.id != responsible.id){
					notResponsible = s;
					break;
				}
			}
			
			//connect to the not responsible server
			System.out.println("Connecting to "+notResponsible);
			Client client = new Client(notResponsible.ipAddress, notResponsible.port);
			KVMessage response = client.getResponse();
			assertEquals("CONNECT_SUCCESS", response.getStatus());
			
			System.out.println("Doing put "+notResponsible);
			client.sendMessage(new MessageType("put","","0","0"));
			response = client.getResponse();
			assertEquals("SERVER_NOT_RESPONSIBLE", response.getStatus());
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that the KVStore can handle SERVER_NOT_RESONSIBLE messages and still connect to the
	 * correct server and do the requests correctly.
	 */
	@Test
	public void testNotResponsibleWithKVStore() {
		System.out.println("Starting testNotResponsibleWithKVStore");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			System.out.println("Done initializing ECS");
			
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			System.out.println("Setting ECS nodes");
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				testECSInstance.removeNode(firstId);
			}
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			testECSInstance.addNode(6,  500, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			Server responsible = testECSInstance.getMetaData().getResponsible("0");
			//find another server which is different from the responsible one
			Server notResponsible = null;
			for (Server s : servers) {
				if (s.id != responsible.id){
					notResponsible = s;
					break;
				}
			}
			
			//connect to the not responsible server
			System.out.println("Connecting to "+notResponsible);
			KVStore kvstore = new KVStore(notResponsible.ipAddress, notResponsible.port);
			kvstore.connect();
			
			//make sure put works
			System.out.println("Doing put");
			KVMessage response = kvstore.put("1", "1");
			assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
			
			//make sure get works
			System.out.println("Doing get");
			response = kvstore.get("1");
			assertEquals("GET_SUCCESS",response.getStatus());
			assertEquals("1",response.getValue());
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that client 1 can put a value and then client 2, which is initially connected
	 * to a different server, can read it.
	 */
	@Test
	public void testPutAndGetMultipleClients() {
		System.out.println("Starting testPutAndGetMultipleClients");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				testECSInstance.removeNode(firstId);
			}
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			testECSInstance.addNode(6,  500, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			//connect to the one server
			Server server1 = allServers.get(4);
			System.out.println("Connect to server "+server1.toString());
			KVStore kvstore1 = new KVStore(server1.ipAddress, server1.port);
			kvstore1.connect();			
			
			//connect to a different server as another client
			Server server2 = allServers.get(6);
			System.out.println("Connect to server "+server2.toString());
			KVStore kvstore2 = new KVStore(server2.ipAddress, server2.port);
			kvstore2.connect();		
			
			//client 1 puts the value
			System.out.println("Client 1 doing put");
			KVMessage response = kvstore1.put("5","5");
			assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
			
			//client 2 gets the value
			System.out.println("Client 2 doing get");
			response = kvstore2.get("5");
			assertEquals("GET_SUCCESS",response.getStatus());
			assertEquals("5",response.getValue());
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * This tests that when the node which the client is currently connected to is removed,
	 * the client is able to connect to a different node and still do the request
	 */
	@Test
	public void testNodesRemovedFromClient() {
		System.out.println("Starting testNodesRemovedFromClient");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				testECSInstance.removeNode(firstId);
			}
			System.out.println("Doing add nodes");
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			testECSInstance.addNode(6,  500, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			//connect to the server 4
			Server server = allServers.get(4);
			System.out.println("Connect to server "+server.toString());
			KVStore kvstore = new KVStore(server.ipAddress, server.port);
			kvstore.connect();			
			
			//remove the server it's connected to
			System.out.println("Removing server "+server.toString());
			testECSInstance.removeNode(server.id);
			
			//do request
			System.out.println("Client doing put request");
			KVMessage response = kvstore.put("6","6");
			assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that the data is redistributed correctly when servers are added and removed
	 */
	@Test
	public void testAddAndRemoveTransfersData() {
		System.out.println("Starting TestAddAndRemoveTransfersData");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			if (servers.size() != 0){ //this can happen if ECS failed to connect to the newly initialized server
				int firstId = servers.get(0).id;
				testECSInstance.removeNode(firstId);
			}
			System.out.println("Adding nodes 0,1,2,3");
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(1,  100, "FIFO");
			testECSInstance.addNode(2,  100, "FIFO");
			testECSInstance.addNode(3,  100, "FIFO");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			//connect to the first server
			Server server = servers.get(0);
			populateStorage(server,0,10);
			
			System.out.println("Adding and removing nodes to redistribute the data");
			testECSInstance.addNode(4,  100, "FIFO");
			if (servers.size() >= 2)
				testECSInstance.removeNode(servers.get(1).id);
			testECSInstance.addNode(5,  100, "FIFO");
			if (servers.size() >= 3)
				testECSInstance.removeNode(servers.get(2).id);
			testECSInstance.addNode(6,  100, "FIFO");
			if (servers.size() >= 4)
				testECSInstance.removeNode(servers.get(3).id);
			testECSInstance.addNode(7,  100, "FIFO");
			
			testECSInstance.start();
			
			//make sure kvstore still reads it
			KVStore kvstore = new KVStore(server.ipAddress, server.port);
			for (int i=0; i<10; i++) {
				System.out.println("Client doing read "+i);
				KVMessage response = kvstore.get(String.valueOf(i));
				assertEquals("GET_SUCCESS",response.getStatus());
				assertEquals(String.valueOf(i),response.getValue());
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that when ECS is shutdown and then restarted with a different set of servers,
	 * all data is maintained
	 */
	@Test
	public void testECSPersistency() {
		System.out.println("Starting testECSPersistency");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(4, 10, "FIFO");
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			//try to connect to one of the servers
			Server s = servers.get(0);
			System.out.println("Connect to server "+s.toString());
			KVStore kvstore = new KVStore(s.ipAddress, s.port);
			kvstore.connect();	
			
			//do some puts
			KVMessage response;
			for (int i=10; i<20; i++) {
				System.out.println("Client doing put "+i);
				response = kvstore.put(String.valueOf(i),String.valueOf(i));
				assertTrue(response.getStatus().equals("PUT_UPDATE") || response.getStatus().equals("PUT_SUCCESS") );
			}	
			
			//shutdown ECS
			System.out.println("Shutting down ECS");
			testECSInstance.shutDown();
			//make sure all servers are shutdown before proceeding
			for (Server server : allServers) {
				boolean ok = serverShutDown(server,10);
				if (!ok){
					System.out.println("ECS shutdown did not kill server "+server.toString());
					//try forcing shutdown again
					killServer(server);
					ok = serverShutDown(server,10);
					if (!ok) {
						System.out.println("WARNING: Server "+server.toString()+" does not appear to be shutting down");
					}
				}
			}
			
			//restart the ECS. The set of servers online will very likely be different this time
			System.out.println("Initializing new ECS");
			testECSInstance = new app_kvEcs.ECS("ecstest.config");
			testECSInstance.initService(4, 20, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			TimeUnit.SECONDS.sleep(2);
			
			//try to connect to one of the servers
			s = servers.get(0);
			System.out.println("Connect to server "+s.toString());
			kvstore = new KVStore(s.ipAddress, s.port);
			kvstore.connect();	
			
			//do gets and make sure kvstore still reads it
			System.out.println("Doing gets");
			for (int i=10; i<20; i++) {
				System.out.println("Client doing read "+i);
				response = kvstore.get(String.valueOf(i));
				assertEquals("GET_SUCCESS",response.getStatus());
				assertEquals(String.valueOf(i),response.getValue());
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	
	///////////////////////////////////////////////////////////////////////////////////////////
	// Tests added specifically for milestone 3 (more in AdditionalTest.java)
	///////////////////////////////////////////////////////////////////////////////////////////
	
	/** 
	 * Tests that ECS.addNode correctly transfers all the data to all primaries AND REPLICAS
	 * as the responsibilities change due to the new node.
	 */
	@Test
	public void testAddNodeTransfersReplicatedData() {
		System.out.println("Starting testAddNodeTransfersReplicatedData");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(7, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();
			
			populateStorage(servers.get(0), 20, 40);
			
			//Since we initialized with all but one server, this will always add the last one
			System.out.println("Adding node");
			testECSInstance.addRandomNode(10,"LRU");
			metadata = testECSInstance.getMetaData();
			
			//Try to get data from every server. Rather than using a KVStore we connect directly to the 
			//servers so we can make sure each one has the data we expect.
			System.out.println("Getting back data");
			for (Server server : allServers) {
				Client client = new Client(server.ipAddress, server.port);
				assertEquals("CONNECT_SUCCESS", client.getResponse().getStatus());
				
				for (int i=20; i<40; i++) {
					String key = String.valueOf(i);
					client.sendMessage(new MessageType("get","",key,""));
					KVMessage response = client.getResponse();
					if (metadata.canGet(server.ipAddress, server.port, key)) {
						assertEquals("GET_SUCCESS",response.getStatus());
						assertEquals(key, response.getValue());
					}
					else {
						assertEquals("SERVER_NOT_RESPONSIBLE", response.getStatus());
					}
				}				
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that ECS.removeNode correctly transfers all the data to all primaries AND REPLICAS
	 * as the responsibilities change due to the new node.
	 */
	@Test
	public void testRemoveNodeTransfersReplicatedData() {
		System.out.println("Starting testRemoveNodeTransfersReplicatedData");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(7, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();
			
			populateStorage(servers.get(0), 40, 60);
			
			System.out.println("Removing node");
			testECSInstance.removeNode(servers.get(1).id);
			metadata = testECSInstance.getMetaData();
			servers = metadata.getAllServers();
			
			//Try to get data from every server. Rather than using a KVStore we connect directly to the 
			//servers so we can make sure each one has the data we expect.
			System.out.println("Getting back data");
			for (Server server : servers) {
				Client client = new Client(server.ipAddress, server.port);
				assertEquals("CONNECT_SUCCESS", client.getResponse().getStatus());
				
				for (int i=40; i<60; i++) {
					String key = String.valueOf(i);
					client.sendMessage(new MessageType("get","",key,""));
					KVMessage response = client.getResponse();
					if (metadata.canGet(server.ipAddress, server.port, key)) {
						assertEquals("GET_SUCCESS",response.getStatus());
						assertEquals(key, response.getValue());
					}
					else {
						assertEquals("SERVER_NOT_RESPONSIBLE", response.getStatus());
					}
				}				
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that the failure detector correctly identifies when a server has crashed
	 */
	@Test
	public void testFailureDetect() {
		System.out.println("Starting testFailureDetect");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();	
			assertEquals(8, servers.size());
			
			//kill some servers
			System.out.println("Killing servers");
			List<Server> killedServers = new ArrayList<Server>();
			killedServers.add(servers.get(0));
			killedServers.add(servers.get(2));
			killedServers.add(servers.get(3));
			killedServers.add(servers.get(7));
			for (Server server : killedServers) {
				killServer(server);
			}
			//make sure servers have died before continuing (takes some time)
			for (Server server : killedServers) {
				while (!serverShutDown(server, 5)) {
					killServer(server);
				}
			}
			
			System.out.println("Checking for failures");
			List<Server> failedServers = testFailureDetect.detectFailures();
			//make sure the list of failed servers is the same as the ones we killed
			assertEquals(killedServers.size(), failedServers.size());
			for (Server server : failedServers) {
				assertTrue(failedServers.contains(server));
			}
			
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that the two ECS instances (in ECSClient and ECSFailureDetect) are able to share
	 * the same metadata via a hard disk file + lock. i.e. when one of them updates it the other 
	 * one sees the update and updates its own. 
	 */
    @Test
    public void testSharedMetadata() {
    	System.out.println("Starting testSharedMetadata");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(2, 10, "FIFO");
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();
			
			//Kill one of the servers. Eventually the ECSFailureDetect should see this and update the metadata.
			Server killed = servers.get(0);
			killServer(killed);
			
			//Now query the other ECS's metadata until we see that it has updated
			int numTries = 5;
			boolean success = false;
			while (numTries-- > 0) {
				System.out.println("Running failure detector");
				List<Server> failedServers = testFailureDetect.detectFailures();
				testFailureDetect.restoreService(failedServers);
				HashRing newMetadata = testECSInstance.getMetaData();
				if (newMetadata.contains(killed)) {
					TimeUnit.SECONDS.sleep(3);
				} else {
					success = true;
					break;
				}
			}
			assertTrue(success);
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
    }
	
	/**
	 * Tests that the failure detector can correctly start new servers to replace failed ones
	 */
	@Test
	public void testFailureDetectorStartsNewServer() {
		System.out.println("Starting testFailureDetectorStartsNewServer");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();	
			assertEquals(8, servers.size());
			
			//kill some servers
			System.out.println("Killing servers");
			List<Server> killedServers = new ArrayList<Server>();
			killedServers.add(servers.get(4));
			killedServers.add(servers.get(6));
			for (Server server : killedServers) {
				killServer(server);
			}
			//make sure servers have died before continuing (takes some time)
			for (Server server : killedServers) {
				while (!serverShutDown(server, 5)) {
					killServer(server);
				}
			}
			
			System.out.println("Restoring failed servers");
			boolean success = testFailureDetect.restoreService(killedServers);
			assertTrue(success);
			
			//Make sure that we can connect to the new servers. Although in general it's
			//not guaranteed that the particular servers which failed will be restored 
			//(any random node is chosen), in this case they will because those are the only
			//other servers in the system. 
			for (Server server : killedServers) {
				assertTrue(canConnect(server));
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/**
	 * Tests that when a node crashes and the failure detector tries to restore the replication 
	 * invariant, the correct parts of the failed node's data are transferred to each of its 3 successors
	 */
	@Test
	public void testFailureDetectorPreservesData() {
		System.out.println("Starting testFailureDetectorPreservesData");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();	
			assertEquals(8, servers.size());
			//using localhost and ports 60000..60007, metadata order is
			//5, 1, 7, 4, 6, 2, 3, 0
			System.out.println(metadata.toString());
			
			populateStorage(servers.get(0), 60, 100);
			
			//kill server 0 
			List<Server> killedServers = new ArrayList<Server>();
			Server killed = servers.get(7); //this is server 0 by id (last in metadata)
			killedServers.add(killed);
			killServer(killed);
			//make sure servers have died before continuing (takes some time)
			while (!serverShutDown(killed, 5)) {
				killServer(killed);
			}
			
			System.out.println("Doing failure recovery");
			testFailureDetect.restoreService(killedServers, false); //false argument tells it not to launch new servers
			
			Server predecessor1 = metadata.getPredecessor(killed);
			Server predecessor2 = metadata.getPredecessor(predecessor1);
			Server successor1 = metadata.getSuccessor(killed);
			Server successor2 = metadata.getSuccessor(successor1);
			Server successor3 = metadata.getSuccessor(successor2);

			//query the metadata to find who is responsible for each key
			for (int i=60; i<100; i++) {
				String x = String.valueOf(i);
				if (metadata.getResponsible(x).equals(killed)) {
					//In this case all 3 successors should have the pair (x,x)
					queryServer(successor1, x, x);
					queryServer(successor2, x, x);
					queryServer(successor3, x, x);
				}
				else if (metadata.getResponsible(x).equals(predecessor1)) {
					//In this case only the first 2 successors should have the pair (x,x)
					queryServer(successor1, x, x);
					queryServer(successor2, x, x);
				}
				else if (metadata.getResponsible(x).equals(predecessor2)) {
					//In this case only the first successor should have the pair (x,x)
					queryServer(successor1, x, x);
				}
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
	
	/** 
	 * Tests that the failure detector is still able to restore all data when 2 consecutive servers fail
	 * (any more than this would be in principle impossible to restore all data due to 2x replication).
	 * This test is made more rigorous by killing servers in the pattern XXOOOXXO, where X is a killed server 
	 * and O is an alive server. Thus the last server has lost both predecessors and both successors, and the third
	 * and last servers are the only replicas for their parts of the data.
	 */
	@Test
	public void testConsecutiveServersFail() {
		System.out.println("Starting testConsecutiveServersFail");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			testECSInstance.start();
			HashRing metadata = testECSInstance.getMetaData();
			List<Server> servers = metadata.getAllServers();	
			assertEquals(8, servers.size());
			//using localhost and ports 60000..60007, metadata order is
			//5, 1, 7, 4, 6, 2, 3, 0
			System.out.println(metadata.toString());
			
			populateStorage(servers.get(0), 100, 120);
			
			System.out.println("Killing servers");
			List<Server> killedServers = new ArrayList<Server>();
			killedServers.add(servers.get(0));
			killedServers.add(servers.get(1));
			killedServers.add(servers.get(5));
			killedServers.add(servers.get(6));
			for (Server server : killedServers) {
				killServer(server);
			}
			//make sure servers have died before continuing (takes some time)
			for (Server server : killedServers) {
				while (!serverShutDown(server, 5)) {
					killServer(server);
				}
			}
			
			System.out.println("Doing failure recovery");
			testFailureDetect.restoreService(killedServers, false); //false argument tells it not to launch new servers
			
			//To simplify the implementation we just use the updated metadata to determine
			//which servers should have each key, and then query those servers for it. 
			//This assumes that metadata queries are 100% correct, as verified by other tests. 
			
			//create copy of metadata
			HashRing updatedMetadata = new HashRing();
			for (Server server : allServers) {
				updatedMetadata.addServer(server);
			}
			//remove killed servers
			for (Server server : killedServers) {
				updatedMetadata.removeServer(server);
			}
			List<Server> remainingServers = metadata.getAllServers();

			//query the metadata to find who is responsible for each key
			for (int i=100; i<120; i++) {
				String x = String.valueOf(i);
				for (Server server : remainingServers) {
					if (updatedMetadata.canGet(server.id, x)) {
						queryServer(server, x, x);
					}
				}
			}
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}
}
