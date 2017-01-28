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
	
	public void testConstructSimple(){
		MessageType message = new MessageType("get"," ","key","value");
		assertNull(message.error);
		assertEquals("get",message.getHeader());
		assertEquals("key",message.getKey());
		assertEquals("value",message.getValue());
		assertEquals(message.getMsg(), "\"get\" \" \" \"key\" \"value\"");
	}
	
	public void testConnectInvalid(){
		MessageType message = new MessageType("connect","localhost","5000"," ");
		assertEquals("Incorrect number of tokens for message put",message.error);
	}
	
	public void testPutInvalid(){
		MessageType message = new MessageType("put"," ","key"," ");
		assertEquals("Incorrect number of tokens for message put",message.error);
	}
	
	public void testSpacesAndQuotesInStrings(){
		MessageType message = new MessageType("get"," ","this is the \"key\"","this is the\"\" value");
		assertNull(message.error);
		assertEquals("get",message.getHeader());
		assertEquals("this is the \"key\"",message.getKey());
		assertEquals("this is the\"\" value",message.getValue());
		assertEquals(message.getMsg(), "\"get\" \" \" \"this is the \"\"key\"\"\" \"this is the\"\"\"\" value\"");
	}
	
	//These test are out of date
	/*public void testDisconnectInvalid(){
		MessageType message = new MessageType("disconnect"," ","key", false);
		assertEquals("Incorrect number of tokens for message disconnect",message.error);
	}
	
	public void testConstructWithStatus(){
		MessageType message = new MessageType("get GET_SUCCESS key1 value1", true);
		assertNull(message.error);
		assertEquals("get",message.getHeader());
		assertEquals("GET_SUCCESS",message.getStatus());
		assertEquals("key1",message.getKey());
		assertEquals("value1",message.getValue());
	}
	
	public void testConstructWithStatusInvalid(){
		MessageType message = new MessageType("put","PUT_SUCCESS","key1"," ");
		assertEquals("Incorrect number of tokens for message put",message.error);
	}
	
	public void testConvertToBytes(){
		MessageType message = new MessageType("get"," ","key1"," ");
		assertNull(message.error);
		byte[] bytes = message.getMsgBytes();
		Assert.assertArrayEquals(new byte[]{103,101,116,32,107,101,121,49,10}, bytes);
	}
	
	public void testConstructFromBytesClientToServer(){
		byte[] bytes = new byte[]{103,101,116,32,107,101,121,49}; 
		MessageType message = new MessageType(bytes, false);
		assertNull(message.error);
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
		
	}*/
}
