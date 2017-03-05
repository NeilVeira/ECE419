package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import common.messages.KVAdminMessage;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import common.HashRing;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			KVServer server = new KVServer(50000, 10, "LRU", 0);
			server.startServer();
			HashRing metadata = new HashRing("-134847710425560069445028245650825152028 localhost 50000 0");
			server.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
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
		clientSuite.addTestSuite(KVStoreTest.class); 
		//clientSuite.addTestSuite(AdditionalTest.class); 
		return clientSuite;
	}
	
}
