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

	// Tests connecting using the command line handler
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

	// Tries puts and gets within the cache size
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
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 200; i++) {			

			try {
				response = kvClient.get(String.valueOf(i));
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
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 500; i++) {			

			try {
				response = kvClient.get(String.valueOf(i));
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
				response = kvClient.put(String.valueOf(i), String.valueOf(Math.random()));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("PUT_UPDATE PUT_SUCCESS".contains(response.getStatus()));
		}
		for(int i = 0; i < 800; i++) {			

			try {
				response = kvClient.get(String.valueOf(i));
			} catch (Exception e) {
				ex = e;
			}

			assertNull(ex);
			assertTrue("GET_SUCCESS".contains(response.getStatus()));
		}
	}
}
