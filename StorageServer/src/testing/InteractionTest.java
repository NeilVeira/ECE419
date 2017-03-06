package testing;

import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;
import junit.framework.TestCase;
import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import java.util.concurrent.TimeUnit;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private KVServer base;
	
	public void setUp() {
		base = new KVServer(50000, 10, "LRU", 0);
		HashRing metadata = new HashRing("-134847710425560069445028245650825152028 localhost 50000 0");
		base.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
		while(base.getStatus() != "ACTIVE") base.startServer();
		
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		//base.closeServer();
	}
	
	// Tests the put function
	@Test
	public void testPut() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		//assertEquals(response.getStatus(),"PUT_SUCCESS");
	}
	
	// Tests put when client is disconnected, connectResponsible should analyze the key and connect the client to the responsible server
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "disconnected";
		Exception ex = null;

		try {
			kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
			System.out.println(ex.toString());
		}
		
		assertNull(ex);
	}

	// Tests put on an already stored value, should return PUT_UPDATE
	@Test
	public void testUpdate() {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, initialValue);
			response = kvClient.put(key, updatedValue);
			
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(response.getStatus(),"PUT_UPDATE");
		assertEquals(response.getValue(), updatedValue);
	}
	
	// Puts using the value "null" should result in a delete
	@Test
	public void testDelete() {
		String key = "deleteTestValue";
		String value = "toDelete";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			kvClient.put(key, value);
			response = kvClient.put(key, "null");
			
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(response.getStatus(), "DELETE_SUCCESS");
	}
	
	// Tests the get function
	@Test
	public void testGet() {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;
		Exception ex = null;

			try {
				kvClient.put(key, value);
				response = kvClient.get(key);
			} catch (Exception e) {
				ex = e;
			}
		
		assertNull(ex);
		assertEquals(response.getValue(), "bar");
	}

	// Tests get on an unset value
	@Test
	public void testGetUnsetValue() {
		String key = "an unset value";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(response.getStatus(), "GET_ERROR");
	}
	


}
