package testing;

import java.net.ConnectException;

import app_kvServer.KVServer;

import client.KVStore;
import java.util.*;
import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	private List<KVServer> servers;
	
	public void setUp() {
		servers = AllTests.createAndStartServers(1, 51300);
	}
	
	public void tearDown() {
		AllTests.closeServers(servers);
		AllTests.deleteLocalStorageFiles();	
		try {
			Thread.sleep(1000); //need to delay a bit between tests because it takes some time for servers to release ports
		} catch (Exception e) {}
	}
	
	// Tests if successful connection can occur
	public void testConnectionSuccess() {
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 51300);
		try {
			boolean success = kvClient.connect();
			assertTrue(success);
		} catch (Exception e) {
			ex = e;
		}	
		assertNull(ex);
	}

	// Tests for multiple connections to different ports
	public void testConnectionMultiple() {
		KVStore[] kvstores = new KVStore[10];
		for (int i=0; i<10; i++) {
			Exception ex = null;
			
			kvstores[i]= new KVStore("localhost", 51300);
			try {
				boolean success = kvstores[i].connect();
				assertTrue(success);
			} catch (Exception e) {
				ex = e;
			}	
			
			if(ex != null) {
				assertTrue(ex instanceof ConnectException);
			} else {
				assertNull(ex);
			}
		}
		
		//disconnect all clients
		for (int i=0; i<10; i++){
			kvstores[i].disconnect();
		}
	}
	
	// Tries to connect to unknown host, should raise exception
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 51300);
		
		try {
			boolean success = kvClient.connect();
			assertFalse(success);
		} catch (Exception e) {
			ex = e; 
		}
		assertNull(ex);
	}
	
	// Tries to connect to a port out of range
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			boolean success = kvClient.connect();
			assertFalse(success);
		} catch (Exception e) {
			ex = e; 
		}
		assertNull(ex);
	}

	// Tries to connect to a port out of range (negative number)
	public void testIllegalPortNegative() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", -1);
		
		try {
			boolean success = kvClient.connect();
			assertFalse(success);
		} catch (Exception e) {
			ex = e; 
		}
		assertNull(ex);
	}
}

