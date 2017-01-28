package testing;

import java.net.UnknownHostException;
import java.net.ConnectException;

import app_kvClient.KVClient;

import client.KVStore;

import junit.framework.TestCase;


public class ConnectionTest extends TestCase {
	
	
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

	
	public void testConnectionMultiple() {
		for (int i = 50001; i < 51000; i++) {
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

