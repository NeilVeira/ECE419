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

import org.junit.Test;

import client.KVStore;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import java.io.FileReader;
import java.lang.StringBuffer;

import junit.framework.TestCase;

public class EnronTest extends TestCase {

	public void testEmails() {
		// Declare Variables for easy modification later on
		// ECS Server Variables
		int InitNumServers = 8;
		int ServerCacheSize = 1000;
		String ServerCacheStrategy = "FIFO";
		
		// Start ECS by creating an ECS instance and manually running its functions rather than using the ECS client
		// Use our ecstest.config file with ports on the 60000 range
		try {
			app_kvEcs.ECS testECSInstance = new app_kvEcs.ECS("ecstest.config");
			// Run ECS initService to start the servers
			System.out.println("initializing Servers");
			testECSInstance.initService(InitNumServers, ServerCacheSize, ServerCacheStrategy);
			testECSInstance.start();
			
			// Read the file emails.csv that is the Enron emails data
			// Use each line as key/value
			// Read until a space, that is the key, rest is value
			FileReader fr;
			try {
				fr = new FileReader("/nfs/ug/homes-1/z/zhaozhi4/419m1/ECE419/StorageServer/emails.csv");
			} catch (IOException e) {
				System.out.println(e.toString());
				assertEquals(false, true);
				return;
			}
			
			// Create new clients and run commands using the Enron data
			KVStore client = new KVStore("localhost", 60000);
			client.connect();
			int data = fr.read();
			// Read each char into a buffer
			StringBuffer read_value = new StringBuffer();
			int size = 0;
			boolean readingKey = true;
			while(data != -1) {
				// Cannot exceed the 20 char limit for keys
				if(data >= 32 && size < 20) {
					read_value.append((char)data);
					size++;
				} else {
					size = 0;
					// Convert buffer to string
					String str = read_value.toString();
					// Clear the buffer
					read_value.delete(0, read_value.length());
					// Process the put command
					if(str.contains(" ")) {
						client.put(str.substring(0, str.indexOf(' '))+".", "."+str.substring(str.indexOf(' ')));
						assertEquals("."+str.substring(str.indexOf(' ')), client.get(str.substring(0, str.indexOf(' '))+".").getValue());
					} else {
						if(str.length() > 19) {
							client.put(str.substring(0, 19), String.valueOf(str.length()));
							assertEquals(String.valueOf(str.length()), client.get(str.substring(0, 19)).getValue());
						} else {
							client.put(str+".", "asdf");
							assertEquals("asdf", client.get(str+".").getValue());
						}
					}
				}
				data = fr.read();
			}
			fr.close();
			testECSInstance.shutDown();
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
