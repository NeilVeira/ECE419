package testing;

import java.io.IOException;

import org.junit.Test;

import client.KVStore;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;


import junit.framework.TestCase;

public class AdditionalTest extends TestCase {

	private KVStore kvClient;
	private Exception ex;
	private KVMessage response;

	public void setUp() {
		KVServer base = new KVServer(50000, 10, "LRU", 0);
		while(base.getStatus() != "ACTIVE") base.startServer();
		HashRing metadata = new HashRing("-134847710425560069445028245650825152028 localhost 50000 0");
		base.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
		
		response = null;
		ex = null;
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
	}

	// Tests connecting using the command line handler
	public void testHandleConnect() {
		KVClient app = new KVClient();
		try {
			app.handleCommand("connect localhost 51234");
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
	}
	
	public void testQuoteInValue(){	
		try{	
			//write value with quotes
			response = kvClient.put("key", "\"a\"\"bc\"");
			//could be update or success depending on whether the test has been run before
			assertTrue(response.getStatus().equals("PUT_UPDATE")|| response.getStatus().equals("PUT_SUCCESS")); 
			
			//read value back
			response = kvClient.get("key");
			assertEquals(response.getStatus(),"GET_SUCCESS"); 
			assertEquals(response.getValue(), "\"a\"\"bc\"");
		}
		catch (Exception e) {
			ex = e;
		}
		assertNull(ex);
	}
	
	public void testDeleteExists() {		
		try {
			//put the key-value
			response = kvClient.put("key",  "value");
			assertTrue(response.getStatus().equals("PUT_UPDATE")|| response.getStatus().equals("PUT_SUCCESS")); 
			
			//try to delete it
			response = kvClient.put("key", "null");
			assertEquals(response.getStatus(),"DELETE_SUCCESS");
			
			//make sure it's actually gone
			response = kvClient.get("key");
			assertEquals(response.getStatus(),"GET_ERROR");
		}
		catch (Exception e) {
			ex = e;
		}
		assertNull(ex);
	}
	
	public void testDeleteDoesNotExist() {	
		try{	
			//delete key which does not exist. Should return DELETE_SUCCESS status. 
			response = kvClient.put("123456789", "null");
			assertEquals(response.getStatus(),"DELETE_SUCCESS");
			//make sure key does not exist
			response = kvClient.get("123456789");
			assertEquals(response.getStatus(),"GET_ERROR");
		}
		catch (Exception e){
			ex = e;
		}
		assertNull(ex);
	}
	
	public void testMultipleClientsAgree() {
		KVStore client2 = new KVStore("localhost",50000);
		try{
			client2.connect();
			//client 1 does a put
			response = kvClient.put("key1", "abc");
			assertTrue(response.getStatus().equals("PUT_UPDATE")|| response.getStatus().equals("PUT_SUCCESS")); 
			
			//client 2 does a get for the same key. Should get the value just written.
			response = client2.get("key1");
		}
		catch (Exception e){
			ex = e;
		}
		assertNull(ex);
		assertEquals(response.getStatus(), "GET_SUCCESS");
		assertEquals(response.getValue(), "abc");
	}
	
	@Test
	public void testPersistence() {
		//create a new server and client and connect to it
		KVServer server = new KVServer(50001, 10, "LFU", 0);
		while(server.getStatus() != "ACTIVE") server.startServer();
		KVStore client = new KVStore("localhost", 50000);
		
		try {
			client.connect();
			
			//first delete the key to make sure this test always starts from the same state
			response = client.put("key", "null");
			assertEquals(response.getStatus(), "DELETE_SUCCESS");
			//now write it
			response = client.put("key", "1010");
			assertTrue(response.getStatus().equals("PUT_UPDATE")|| response.getStatus().equals("PUT_SUCCESS")); 
			
			//disconnect and kill the server
			client.disconnect();
			server.closeServer();
			
			//start up a new server and reconnect
			KVServer server2 = new KVServer(50001, 10, "LFU", 0);
			while(server2.getStatus() != "ACTIVE") server2.startServer();
			client.connect();
			
			//get the value. Should be the same as put.
			response = client.get("key");
		}
		catch (Exception e){
			ex = e;
		}
		assertEquals(response.getStatus(), "GET_SUCCESS");
		assertEquals(response.getValue(), "1010");
		assertNull(ex);
	}
	
	/*// Tries puts and gets within the cache size
	public void testPutGetSmall() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 8; i++) {			

			try {
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 8; i++) {			

			try {
				response = kvClient.get(String.valueOf(i));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}
	
	// Tries puts and gets larger than the cache size
	public void testPutGetLarge() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 20; i++) {			

			try {
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 20; i++) {			

			try {
				response = kvClient.get(String.valueOf(i));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}

	// Tries puts and gets larger than the cache size but still obeys temporal locality
	public void testPutGetLargeCached() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 20; i++) {			

			try {
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 19; i >= 0; i--) {			

			try {
				response = kvClient.get(String.valueOf(i));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}

	// Tries puts and gets very large number (all valid)
	public void testPutGetVeryLarge() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 200; i++) {			

			try {
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 199; i >= 0; i--) {			

			try {
				response = kvClient.get(String.valueOf(i));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}

	// Times 800 puts and 200 gets
	public void testPutGet800200() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 800; i++) {			

			try {
				response = kvClient.put(String.valueOf(((int)(Math.random()*500)%10)), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 200; i++) {			

			try {
				response = kvClient.get(String.valueOf(((int)(Math.random()*500)%10)));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}

	// Times 500 puts and 500 gets
	public void testPutGet500500() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 500; i++) {			

			try {
				response = kvClient.put(String.valueOf(((int)(Math.random()*500)%10)), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 500; i++) {			

			try {
				response = kvClient.get(String.valueOf(((int)(Math.random()*500)%10)));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}

	// Times 200 puts and 800 gets
	public void testPutGet200800() {
		Exception ex = null;
		KVMessage response = null;

		for(int i = 0; i < 200; i++) {			

			try {
				response = kvClient.put(String.valueOf(((int)(Math.random()*500)%10)), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 800; i++) {			

			try {
				response = kvClient.get(String.valueOf(((int)(Math.random()*500)%10)));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}*/
}
