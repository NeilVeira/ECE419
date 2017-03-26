package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import common.HashRing.Server;
import common.messages.KVAdminMessage;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import common.*;
import common.HashRing.*;
import common.messages.*;
import java.io.File;
import java.util.*;

public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.DEBUG);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		//clientSuite.addTestSuite(TestKVMessage.class);
		//clientSuite.addTestSuite(TestHashRing.class);
		//clientSuite.addTestSuite(ConnectionTest.class);
		//clientSuite.addTestSuite(InteractionTest.class); 
		//clientSuite.addTestSuite(KVStoreTest.class); 
		//clientSuite.addTestSuite(AdditionalTest.class); 
		clientSuite.addTestSuite(EnronTest.class); 
		return clientSuite;
	}
	
	/**
	 * Creates and starts numServers servers
	 */
	public static List<KVServer> createAndStartServers(int numServers, int base_port) {
		HashRing metadata = new HashRing();
		List<KVServer> servers = new ArrayList<KVServer>();
		
		for (int i=0; i<numServers; i++) {
			KVServer server = new KVServer(base_port+i, 10, "FIFO", i);
			server.startServer();
			servers.add(server);
			metadata.addServer(new Server("localhost", base_port+i, i));
		}
		System.out.println(metadata.toString());
		
		for (KVServer server : servers) {
			server.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
		}
		
		KVServer s = servers.get(0);
		s.startServer();
		return servers;
	}	
	
	public static List<KVServer> createAndStartServers(int numServers) {
		return createAndStartServers(numServers, 50000);
	}
	
	public static void closeServers(List<KVServer> servers) {
		for (KVServer server : servers) {
			server.closeServer();
		}
	}
	
	/**
	 * Delete all storage files numbered from 0 to 9 on the local path.
	 * Used to make sure tests start from a fresh state. 
	 */
	public static void deleteLocalStorageFiles() {
		for (int i=0; i<10; i++) {
			String fileName = "storage_"+i+".txt";
			File file = new File(fileName);
			file.delete();
		}
	}
}

