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
		KVStore[] kvstores = new KVStore[10];
		for (int i=0; i<10; i++) {
			Exception ex = null;
			
			kvstores[i]= new KVStore("localhost", 50000);
			try {
				kvstores[i].connect();
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

