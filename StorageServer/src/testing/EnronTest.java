package testing;

import java.io.FileNotFoundException;
import java.io.IOException;

import junit.framework.TestCase;

import client.KVStore;

import java.io.FileReader;
import java.lang.StringBuffer;

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
