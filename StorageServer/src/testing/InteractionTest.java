package testing;

import org.junit.Test;

import app_kvServer.KVServer;

import client.KVStore;
import junit.framework.TestCase;
import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import java.util.List;
import java.util.concurrent.TimeUnit;


public class InteractionTest extends TestCase {

	private KVStore kvClient;
	private List<KVServer> servers;
	
	public void setUp() {
		servers = AllTests.createAndStartServers(1, 50000);		
		kvClient = new KVStore("localhost", 50000);
		try {
			kvClient.connect();
		} catch (Exception e) {
		}
	}

	public void tearDown() {
		kvClient.disconnect();
		AllTests.closeServers(servers);
		AllTests.deleteLocalStorageFiles();	
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
		assertEquals(value, response.getValue());
		//assertEquals(response.getStatus(),"PUT_SUCCESS");
	}
	
	// Tests put when client is disconnected, connectResponsible should analyze the key and connect the client to the responsible server
	@Test
	public void testPutDisconnected() {
		kvClient.disconnect();
		String key = "foo";
		String value = "disconnected";
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
			System.out.println(ex.toString());
		}
		
		assertNull(ex);
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		assertEquals(value, response.getValue());
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
			response = kvClient.put(key, initialValue);
			assertEquals(response.getValue(), initialValue);
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
		
		// Use get to verify it is unset
		try {
			response = kvClient.get(key);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertEquals(response.getStatus(), "GET_ERROR");
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
