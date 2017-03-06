package testing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.junit.Test;

import app_kvClient.KVClient;

import app_kvEcs.ECS;
import app_kvEcs.ECSClient;

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
	
	public void setUp() {
		try {
			// Start ECS by creating an ECS instance and manually running its functions rather than using the ECS client
			testECSInstance = new app_kvEcs.ECS("ecstest.config");
			allServers = testECSInstance.getAllServers();
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
	
	public void tearDown() {
		// Reset the metaData and overWrite
		System.out.println(" Cleaning Up");
		testECSInstance.clearMetaData();
		boolean Success = testECSInstance.writeMetadata();
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
		
		//make sure all servers are shutdown before proceeding
		for (Server server : allServers) {
			boolean ok = serverShutDown(server,10);
			if (!ok){
				System.out.println("WARNING: Server "+server+" does not appear to be shutting down");
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
				Client client = new Client(server.ipAddress, server.port);
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
			Client client = new Client(server.ipAddress, server.port);
			return true;
		} catch (IOException e){
			return false;
		}
	}
	
	// Test whether the ECS can properly start a server cluster that allows client connections and shutdown properly
	/*@Test
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
	}*/
	
	 //Tests that all servers reply SERVER_STOPPED to put and get requests when the ECS is initialized
	/*public void testServerStopped() {
		System.out.println("Starting testServerStopped");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(8, 10, "FIFO");
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			
			for (Server server : servers){
				System.out.println("Connecting to server "+server.toString());
				KVStore client = new KVStore(server.ipAddress, server.port);
				//make sure get responds server stopped
				KVMessage response = client.get("0");
				assertEquals("SERVER_STOPPED", response.getStatus());
				//make sure put responds server stopped
				response = client.put("0", "0");
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
	
	// Tests that the ECS can start and stop servers and that they only reply "SERVER_STOPPED" when stopped
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
	
	// tests that removeNode correctly shuts down the server 
	public void testRemoveNode() {
		System.out.println("Starting testRemoveNode");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			Server server = testECSInstance.getMetaData().getAllServers().get(0);
			
			//make sure we can connect now
			try {
				Client client = new Client(server.ipAddress, server.port);
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
	
	//Tests that ECS starts a multi-server system and that the client can do gets and puts
	//This is a simple case where the same client does the get & put
	public void testPutAndGetMultipleServersSimple() {
		System.out.println("Starting testPutAndGetMultipleServersSimple");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			int firstId = servers.get(0).id;
			testECSInstance.removeNode(firstId);
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
			assertEquals("PUT_SUCCESS", response.getStatus());
			
			//read it back and make sure it's correct
			response = client.get("0");
			assertEquals("GET_SUCCESS", response.getStatus());
			assertEquals("0", response.getValue());
		}
		catch (Exception e){
			ex = e;
			System.out.println("Error: "+e.getMessage());
			e.printStackTrace();
		}
		assertNull(ex);
	}*/
	
	//tests that client receives a "SERVER_NOT_RESPONSIBLE" message when doing a put to the wrong server
	
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
			int firstId = servers.get(0).id;
			System.out.println("removing node "+firstId);
			testECSInstance.removeNode(firstId);
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
			System.out.println("Done put. waiting for response");
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
	
	//tests that the KVStore can handle SERVER_NOT_RESONSIBLE messages and still connect to the
	//correct server and do the requests correctly.
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
			int firstId = servers.get(0).id;
			System.out.println("Setting ECS nodes");
			testECSInstance.removeNode(firstId);
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
			
			//make sure put works
			System.out.println("Doing put");
			KVMessage response = kvstore.put("1", "1");
			assertEquals("PUT_SUCCESS",response.getStatus());
			
			//make sure get works
			System.out.println("Doing get");
			response = kvstore.get("0");
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
	
	// Tests that client 1 can put a value and then client 2, which is initially connected
	// to a different server, can read it
	/*public void testPutAndGetMultipleClients() {
		System.out.println("Starting testPutAndGetMultipleClients");
		Exception ex = null;
		try {
			System.out.println("Initializing ECS");
			testECSInstance.initService(1, 10, "FIFO");
			//since initService is random, find out what server it added from the metadata, remove it
			//and then add fixed ones
			List<Server> servers = testECSInstance.getMetaData().getAllServers();
			int firstId = servers.get(0).id;
			testECSInstance.removeNode(firstId);
			testECSInstance.addNode(0, 10, "FIFO");
			testECSInstance.addNode(2, 20, "LRU");
			testECSInstance.addNode(4,  5, "LFU");
			servers = testECSInstance.getMetaData().getAllServers();
			testECSInstance.start();
			
			Server responsible = testECSInstance.getMetaData().getResponsible("0");
			
			//connect to the first server
			Server connectTo = servers.get(0);
			System.out.println("Connect to server "+connectTo);
			KVStore client = new KVStore(connectTo.ipAddress, connectTo.port);
			client.connect();			
			
			//put the value
			KVMessage response = client.put("2", "2");
			assertEquals("PUT_SUCCESS", response.getStatus());
			
			//read it back and make sure it's correct
			response = client.get("0");
			assertEquals("GET_SUCCESS", response.getStatus());
			assertEquals("0", response.getValue());
		}
		catch (Exception e){
			ex = e;
		}
		assertNull(ex);
	}*/
	

}
