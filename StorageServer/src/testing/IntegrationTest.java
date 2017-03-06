package testing;
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

	// Test whether the ECS can properly start a server cluster that allows client connections and shutdown properly
	@Test
	public void testSetupSanityAndShutDownServers() {
		// Declare Variables for easy modification later on
		// ECS Server Variables
		int InitNumServers = 5;
		int ServerCacheSize = 100;
		String ServerCacheStrategy = "FIFO";
		
		// Start ECS by creating an ECS instance and manually running its functions rather than using the ECS client
		// Use our ecstest.config file with ports on the 60000 range
		try {
			app_kvEcs.ECS testECSInstance = new app_kvEcs.ECS("ecstest.config");
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
			// Done Basic Sanity Test Do Clean Up
			// Reset the metaData and overWrite
			System.out.println(" Cleaning Up");
			testECSInstance.clearMetaData();
			boolean Success = testECSInstance.writeMetadata();
			assertEquals(true, Success);
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
	
}
