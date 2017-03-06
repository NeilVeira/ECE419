package testing;

import java.net.UnknownHostException;
import java.net.ConnectException;

import common.HashRing;
import common.messages.KVAdminMessage;

import app_kvClient.KVClient;
import app_kvServer.KVServer;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	
	public void setUp() {
		KVServer base = new KVServer(50000, 10, "LRU", 0);
		while(base.getStatus() != "ACTIVE") base.startServer();
		HashRing metadata = new HashRing("-134847710425560069445028245650825152028 localhost 50000 0");
		base.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
	}
	
	// Tests if successful connection can occur
	public void testConnectionSuccess() {
		Exception ex = null;
		
		KVStore kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e;
		}	
		assertNull(ex);
	}

	// Tests for multiple connections to different ports
	public void testConnectionMultiple() {
		for (int i = 50001; i < 50010; i++) {
			Exception ex = null;
			
			KVStore kvClient = new KVStore("localhost", i);
			try {
				kvClient.connect();
			} catch (Exception e) {
				ex = e;
			}	
			
			if(ex != null) {
				assertTrue(ex instanceof ConnectException);
			} else {
				assertNull(ex);
			}
			
		}
	}
	
	// Tries to connect to unknown host, should raise exception
	public void testUnknownHost() {
		Exception ex = null;
		KVStore kvClient = new KVStore("unknown", 50000);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof UnknownHostException);
	}
	
	// Tries to connect to a port out of range
	public void testIllegalPort() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", 123456789);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}

	// Tries to connect to a port out of range (negative number)
	public void testIllegalPortNegative() {
		Exception ex = null;
		KVStore kvClient = new KVStore("localhost", -1);
		
		try {
			kvClient.connect();
		} catch (Exception e) {
			ex = e; 
		}
		
		assertTrue(ex instanceof IllegalArgumentException);
	}
}

