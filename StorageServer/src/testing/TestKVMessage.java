package testing;

import org.junit.Test;
import org.junit.Assert;

import common.messages.MessageType;

import junit.framework.TestCase;

public class TestKVMessage extends TestCase {
	
	public void setUp() {
	}

	public void tearDown() {
	}
	
	public void testQuitCorrect(){
		MessageType message = new MessageType("quit", false);
		assertTrue(message.isValid);
		assertEquals("quit",message.getHeader());
		assertEquals("quit",message.getMsg());
	}
	
	public void testConnectCorrect(){
		MessageType message = new MessageType("connect localhost 5000", false);
		assertTrue(message.isValid);
		assertEquals("connect",message.getHeader());
		assertEquals("localhost",message.getKey());
		assertEquals("5000",message.getValue());
		assertEquals("connect localhost 5000",message.getMsg());
	}
	
	public void testGetCorrect(){
		MessageType message = new MessageType("get key", false);
		assertTrue(message.isValid);
		assertEquals("get",message.getHeader());
		assertEquals("key",message.getKey());
		assertEquals("get key",message.getMsg());
	}
	
	public void testPutCorrect(){
		MessageType message = new MessageType("put key value1 value2     value3", false);
		assertTrue(message.isValid);
		assertEquals("put",message.getHeader());
		assertEquals("key",message.getKey());
		assertEquals("value1 value2 value3",message.getValue());
		assertEquals("put key value1 value2 value3",message.getMsg());
	}
	
	public void testPutInvalid(){
		MessageType message = new MessageType("put key ", false);
		assertFalse(message.isValid);
		assertEquals("Incorrect number of tokens for message put",message.error);
	}
	
	public void testDisconnectInvalid(){
		MessageType message = new MessageType("disconnect key ", false);
		assertFalse(message.isValid);
		assertEquals("Incorrect number of tokens for message disconnect",message.error);
	}
	
	public void testConstructWithStatus(){
		MessageType message = new MessageType("get GET_SUCCESS key1 value1", true);
		assertTrue(message.isValid);
		assertEquals("get",message.getHeader());
		assertEquals("GET_SUCCESS",message.getStatus());
		assertEquals("key1",message.getKey());
		assertEquals("value1",message.getValue());
	}
	
	public void testConstructWithStatusInvalid(){
		MessageType message = new MessageType("put PUT_SUCCESS key1 ", true);
		assertFalse(message.isValid);
		assertEquals("Incorrect number of tokens for message put",message.error);
	}
	
	public void testConvertToBytes(){
		MessageType message = new MessageType("get key1", false);
		assertTrue(message.isValid);
		byte[] bytes = message.getMsgBytes();
		Assert.assertArrayEquals(new byte[]{103,101,116,32,107,101,121,49,10}, bytes);
	}
	
	public void testConstructFromBytesClientToServer(){
		byte[] bytes = new byte[]{103,101,116,32,107,101,121,49}; 
		MessageType message = new MessageType(bytes, false);
		assertTrue(message.isValid);
		assertEquals("get",message.getHeader());
		assertEquals("key1",message.getKey());
		assertEquals("get key1",message.getMsg());
	}
	
	public void testConstructFromBytesServerToClient(){
		byte[] bytes = new byte[]{103,101,116,32,71,69,84,95,83,85,67,67,69,83,83,32,107,101,121,49,32,118,97,108,117,101,49,10};
		MessageType message = new MessageType(bytes, true);
		assertTrue(message.isValid);
		assertEquals("get",message.getHeader());
		assertEquals("GET_SUCCESS",message.getStatus());
		assertEquals("key1",message.getKey());
		assertEquals("value1",message.getValue());
		assertEquals("get GET_SUCCESS key1 value1",message.getMsg());
		
	}
}
