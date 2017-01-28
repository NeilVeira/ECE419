package testing;

import org.junit.Test;

import client.KVStore;

import common.messages.KVMessage;

import app_kvClient.KVClient;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
	private KVStore kvClient;
	
	public void setUp() {
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}
	
	@Test
	public void testStub() {
		assertTrue(true);
	}
	
	public void testHandleConnect() {
		Exception ex = null;
		KVClient app = new KVClient();
		
		try {
			app.handleCommand("connect localhost 51234");
		} catch (Exception e) {
			ex = e;
		}
		
		assertNull(ex);
	}
	
	public void testPutOverload1000() {
		Exception ex = null;
		KVMessage response = null;
		
		for(int i = 0; i < 1000; i++) {			

			try {
				response = kvClient.put(String.valueOf(Math.random()), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertEquals(response.getStatus(),"PUT_SUCCESS");
		}
		
		
	}
}
