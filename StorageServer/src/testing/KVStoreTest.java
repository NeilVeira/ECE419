package testing;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.Test;

import client.KVStore;
import app_kvClient.KVClient;
import app_kvServer.KVServer;
import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.KVMessage;
import java.util.concurrent.TimeUnit;


import junit.framework.TestCase;
import logger.LogSetup;

public class KVStoreTest extends TestCase {

	private KVStore kvClient;
	private KVServer server;

	public void setUp() {
		server = new KVServer(50001, 10, "FIFO", 1);
		try {
			while(server.getStatus() != "ACTIVE") server.startServer();
			HashRing metadata = new HashRing("136415732930669195156142751695833227657 localhost 50001 1,-134847710425560069445028245650825152028 localhost 50000 0");
			server.handleMetadata(new KVAdminMessage("metadata","METADATA_UPDATE","",metadata.toString()));
		} catch (Exception e) {
			e.printStackTrace();
		}
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
		server.closeServer();
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
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
	}

	// Tests a put on the current server with the wrong position on the ring.
	// Should redirect to port 50000
	@Test
	public void testWrongServer() {
		String key = "01";
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
			while(server.getStatus() != "STOPPED") server.stopServer();
			response = kvClient.put(key, value);
			while(server.getStatus() != "ACTIVE") server.startServer();
		} catch (Exception e) {
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
		System.out.println("YOOOOOOOOOOOOOO");
		KVMessage response = null;
		Exception ex = null;

		try {
			while(server.getStatus() != "WRITE_LOCKED") server.lockWrite();
			response = kvClient.put(key, value);
			/*try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				
			}*/
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
		} catch (Exception e) {
			while(server.getStatus() != "ACTIVE") server.unLockWrite();
			ex = e;
		}

		assertNull(ex);
		assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
	}
}
