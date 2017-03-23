package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import common.messages.KVAdminMessage;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import common.HashRing;
import java.io.File;


public class AllTests {

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			// Delete all the storage files already present for the servers
			/*for(int i = 0; i < 8; ++i) {
				File file = new File(System.getProperty("user.dir") + "/" + "storage_" + i + ".txt");
				file.delete();
			}*/
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(TestKVMessage.class);
		clientSuite.addTestSuite(TestHashRing.class);
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class); 
		clientSuite.addTestSuite(KVStoreTest.class); 
		clientSuite.addTestSuite(AdditionalTest.class); 
		//clientSuite.addTestSuite(EnronTest.class); 
		return clientSuite;
	}
	
}
