package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import client.KVStore;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import common.*;
import common.messages.*;
import common.HashRing.*;
import java.util.concurrent.TimeUnit;
import java.util.*;
import junit.framework.TestCase;
import logger.LogSetup;

public class KVStoreTest extends TestCase {

	private KVStore kvClient;
	private KVServer server;
	private KVServer base;
	private List<KVServer> servers;

	public void setUp() {		
		servers = AllTests.createAndStartServers(2);
		base = servers.get(0);
		server = servers.get(1);

		// We also initialize the client here
		kvClient = new KVStore("localhost", 50001);
		// Blocks until server is ready to connect
		while(kvClient.isConnected() == false) {
			try {
				kvClient.connect();
			} catch (Exception e) {
			}
		}
	}
	
	public void tearDown() {
		kvClient.disconnect();
		AllTests.closeServers(servers);
		AllTests.deleteLocalStorageFiles();		
	}

	// Tests a put on the current server with the right position on the ring
	@Test
	public void testRightServer() {
		String key = "foo";
		String value = "Right";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		System.out.println(response.getStatus());
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
	}

	// Tests a put on the current server with the wrong position on the ring.
	// Should redirect to port 50000
	@Test
	public void testWrongServer() {
		String key = "01";
		// Tested the key so it will allocate to the 50000 server.
		
		//HashRing hashFind = new HashRing();
		//System.out.println(hashFind.objectHash(key).toString());
		String value = "Wrong";

		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertNull(ex);
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
	}
	
	// Tests how KVStore responds when the connected server is in the stopped state
	@Test
	public void testStoppedServer() {
		String key = "foo";
		String value = "Stopped";
		
		KVMessage response = null;
		Exception ex = null;

		try {
			// Verify that the server is stopped before we send out the put
			while(server.getStatus() != "STOPPED") server.stopServer();
			response = kvClient.put(key, value);
			// Have to make sure the server is running again for the other tests
			while(server.getStatus() != "ACTIVE") server.startServer();
		} catch (Exception e) {
			// Make sure the server is running, even if this test causes exception
			while(server.getStatus() != "ACTIVE") server.startServer();
			ex = e;
		}

		assertNull(ex);
		assertTrue("SERVER_STOPPED".contains(response.getStatus()));
	}
	
	// Simulates a write locked server for a couple seconds, KVStore should keep trying until the write lock is over
	@Test
	public void testWLServer() {
		String key = "foo";
		String value = "WL";
		KVMessage response = null;
		Exception ex = null;

		try {
			// Set write lock on the server
			while(server.getStatus() != "WRITE_LOCKED") server.lockWrite();
			// In KVStore it will keep retrying until 10 iterations of 500ms each, 5 seconds total
			response = kvClient.put(key, value);
			// Unlock the server for the other tests
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
		} catch (Exception e) {
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
			ex = e;
		}

		assertNull(ex);
		// KVStore returns PUT_ERROR if write locked server times out
		assertTrue("PUT_ERROR".contains(response.getStatus()));
	}
	
	@Test
	public void testWLGetServer() {
		String key = "foo";
		String value = "WLGet";
		KVMessage response = null;
		Exception ex = null;

		try {
			// Put in a value first
			response = kvClient.put(key, value);
			// Set write lock on the server
			while(server.getStatus() != "WRITE_LOCKED") server.lockWrite();
			// In KVStore it will keep retrying until 10 iterations of 500ms each, 5 seconds total
			response = kvClient.get(key);
			// Unlock the server for the other tests
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
		} catch (Exception e) {
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
			ex = e;
		}

		assertNull(ex);
		// KVStore returns PUT_ERROR if write locked server times out
		assertTrue("GET_SUCCESS".contains(response.getStatus()));
	}
}
