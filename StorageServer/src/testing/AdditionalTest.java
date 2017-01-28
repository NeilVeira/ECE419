package testing;

import org.junit.Test;

import app_kvClient.KVClient;

import junit.framework.TestCase;

public class AdditionalTest extends TestCase {
	
	// TODO add your test cases, at least 3
	
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
}
