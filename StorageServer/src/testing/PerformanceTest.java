package testing;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;
import java.sql.Timestamp;

import junit.framework.TestCase;

import org.junit.Test;

import common.HashRing.Server;

public class PerformanceTest extends TestCase {

	// Test whether the ECS can properly start a server cluster that allows client connections and shutdown properly
	@Test
	public void testPerformance1Storage28RW1Client() {
		// Declare Variables for easy modification later on
		// ECS Server Variables
		int InitNumServers = 5;
		int ServerCacheSize = 10;
		String ServerCacheStrategy = "FIFO";
		// Start ECS by creating an ECS instance and manually running its functions rather than using the ECS client
		// Use our ecstest.config file with ports on the 60000 range
		try {
			app_kvEcs.ECS testECSInstance = new app_kvEcs.ECS("ecstest.config");
			// Run ECS initService to start the servers
			System.out.println("initializing Servers");
			testECSInstance.UnlockMetadata();
			testECSInstance.initService(InitNumServers, ServerCacheSize, ServerCacheStrategy);
			// Find out how many Servers are running on the SSH host by getting the Hash Ring from the ECS class
			long SleepDelay = 1000;
			// Wait 3 seconds for Servers to start
			Thread.sleep(SleepDelay);
			SleepDelay = 1000;
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
			// Start the Servers for operation
			testECSInstance.start();
			Thread.sleep(SleepDelay);
			
			// RUN TEST FOR CURRENT CONFIGURATION
			// Make a new KVStore to test and have it connect to a random Server in the list
			int serverID = (int)Math.random()% testServerList.size();
			testServer = testServerList.get(serverID);
			String ServerAddress = testServer.ipAddress;
			int ServerPort = testServer.port;
			client.KVStore testKVStoreInstance = new client.KVStore(ServerAddress,ServerPort);
			System.out.println("Connecting Client to " + ServerAddress + " at Port " + ServerPort);
			// Connect to Server
			boolean Success = testKVStoreInstance.connect();
			assertEquals(true, Success);
			System.out.println(" Success! Connected to " + ServerAddress + " at Port " + ServerPort);
			// Measure time taken to perform 1000 operations
			// Decide with a 8 to 2 ratio whether next operation will be put or get
			double TimeForPut = 0;
			double TimeForGet = 0;
			double TotalTime = 0;
			double TotalThroughPut = 0;
			int NumGet = 0;
			int NumPut = 0;
			common.messages.KVMessage dummy;
			System.out.println("Start Test loop");
			for (int i = 0; i < 1000; i++) {
				int randomNum = (int)(Math.random()*1000 %10);
				System.out.println("Random Num is " + String.valueOf(randomNum));
				boolean doPut;
				if (randomNum >= 5) {
					doPut = true;
				} else {
					doPut = false;
				}
				
				// Determine the Key to Put or get from 
				if (doPut) {
					System.out.println("Doing Put");
					Timestamp StartTimeStamp = new Timestamp(System.currentTimeMillis());
					dummy = testKVStoreInstance.put(String.valueOf((int)(Math.random()*100)), String.valueOf((int)(Math.random()*100)));
					System.out.println("RECEIVED KVMESSAGE: " + dummy.getHeader() + " " + dummy.getKey() + " " + dummy.getValue() + " " + dummy.getStatus());
					Timestamp EndTimeStamp = new Timestamp(System.currentTimeMillis());
					double TimeUsed = EndTimeStamp.getTime() - StartTimeStamp.getTime();
					NumPut = NumPut + 1;
					TimeForPut = TimeForPut + TimeUsed;
				} else {
					System.out.println("Doing Get");
					Timestamp StartTimeStamp = new Timestamp(System.currentTimeMillis());
					dummy = testKVStoreInstance.get(String.valueOf((int)(Math.random()*100)));
					System.out.println("RECEIVED KVMESSAGE: " + dummy.getHeader() + " " + dummy.getKey() + " " + dummy.getValue() + " " + dummy.getStatus());
					Timestamp EndTimeStamp = new Timestamp(System.currentTimeMillis());
					double TimeUsed = EndTimeStamp.getTime() - StartTimeStamp.getTime();
					NumGet = NumGet + 1;
					TimeForGet = TimeForGet + TimeUsed;
				}
				System.out.println("I is now " + String.valueOf(i));
			}
			System.out.println("End Test Loop");
			TotalTime = TimeForPut + TimeForGet;
			TotalThroughPut = TotalTime / (NumPut + NumGet);
			double AvgTimePut = TimeForPut / NumPut;
			double AvgTimeGet = TimeForGet / NumGet;
			// Output the results to a file
			// Make new results file
			File testPerformance1Storage28RW1Client = new File("testPerformance1Storage28RW1Client.txt");
			try {
				System.out.println("Writing Results to File");
				PrintWriter writer = new PrintWriter(testPerformance1Storage28RW1Client, "UTF-8");
				writer.println("Total Time: " + String.valueOf(TotalTime));
				writer.println("Total ThroughPut: " + String.valueOf(TotalThroughPut));
				writer.println("Average time per Put: " + String.valueOf(AvgTimePut));
				writer.println("Average time per Get: " + String.valueOf(AvgTimeGet));
				writer.close();
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			// Next ShutDown the Servers with ECS and see if Clients can still connect to the servers or not
			System.out.println("Shutting down all servers");
			testECSInstance.shutDown();
			System.out.println("Shutted Down waiting 15 seconds for threads to be killed");
			SleepDelay = 3000;
			// Wait seconds for Servers to shut down
			Thread.sleep(SleepDelay);
			
			// Test that our Client can no longer Connect to the Servers that were up
			// Create a Custom KVStore to directly invoke functions and bypass KVClient
			counter = 0;
			// Get first element of list of servers
			testServer = testServerList.get(counter);
			while (counter < InitNumServers) {
				// Create a Custom KVStore to directly invoke functions and bypass KVClient
				ServerAddress = testServer.ipAddress;
				ServerPort = testServer.port;
				testKVStoreInstance = new client.KVStore(ServerAddress,ServerPort);
				System.out.println("Connecting Client to " + ServerAddress + " at Port " + ServerPort);
				while(testKVStoreInstance.connect()) {
					testECSInstance.shutDown();
					Thread.sleep(SleepDelay);
				}
				//assertEquals(false, Success);
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
			Success = testECSInstance.writeMetadata();
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
