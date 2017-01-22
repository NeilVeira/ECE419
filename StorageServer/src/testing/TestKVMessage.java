package testing;

import org.junit.Test;

import common.messages.MessageType;

import junit.framework.TestCase;

public class TestKVMessage extends TestCase {
	
	public void setUp() {
	}

	public void tearDown() {
	}
	
	public void testQuitCorrect(){
		MessageType message = new MessageType("quit");
		assertEquals("quit",message.getMsg());
		assertEquals("quit",message.getHeader());
		assertTrue(message.isValid);
	}
	
	public void testconnectCorrect(){
		MessageType message = new MessageType("connect localhost 5000");
		assertEquals("connect localhost 5000",message.getMsg());
		assertEquals("connect",message.getHeader());
		assertEquals("localhost",message.getKey());
		assertEquals("5000",message.getValue());
		assertTrue(message.isValid);
	}
	
	public void testGetCorrect(){
		MessageType message = new MessageType("get key");
		assertEquals("get key",message.getMsg());
		assertEquals("get",message.getHeader());
		assertEquals("key",message.getKey());
		assertTrue(message.isValid);
	}
	
	public void testPutCorrect(){
		MessageType message = new MessageType("put key value1 value2     value3");
		assertTrue(message.isValid);
		assertEquals("put key value1 value2     value3",message.getMsg());
		assertEquals("put",message.getHeader());
		assertEquals("key",message.getKey());
		assertEquals("value1 value2 value3",message.getValue());
	}
	
	public void testPutInvalid(){
		MessageType message = new MessageType("put key ");
		assertFalse(message.isValid);
		assertEquals("Incorrect number of tokens for message put",message.error_msg);
	}
	
	public void testDisconnectInvalid(){
		MessageType message = new MessageType("disconnect key ");
		assertFalse(message.isValid);
		assertEquals("Incorrect number of tokens for message disconnect",message.error_msg);
	}
}
