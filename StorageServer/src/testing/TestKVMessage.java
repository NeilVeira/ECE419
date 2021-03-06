package testing;

import org.junit.Test;
import org.junit.Assert;

import app_kvServer.KVServer;

import common.HashRing;
import common.messages.KVAdminMessage;
import common.messages.MessageType;

import junit.framework.TestCase;

public class TestKVMessage extends TestCase {
	
	public void testConstructSimple(){
		MessageType message = new MessageType("get"," ","key","value");
		assertNull(message.error);
		assertEquals("get",message.getHeader());
		assertEquals("key",message.getKey());
		assertEquals("value",message.getValue());
		assertEquals(message.getMsg(), "\"get\" \" \" \"key\" \"value\"");
	}
	
	public void testDisconnectInvalid(){
		MessageType message = new MessageType("disconnect","localhost","5000"," ");
		// TODO: fix this test
		assertEquals("Validity Check: Key and value must be empty for message disconnect",message.error);
	}
	
	public void testPutInvalid(){
		MessageType message = new MessageType("put"," ","key"," ");
		assertEquals("Validity Check: Key and value must not be empty for message put",message.error);
	}
	
	public void testSpacesAndQuotesInStrings(){
		MessageType message = new MessageType("get"," ","this is the \"key\"","this is the\"\" value");
		assertNull(message.error);
		assertEquals("get",message.getHeader());
		assertEquals("this is the \"key\"",message.getKey());
		assertEquals("this is the\"\" value",message.getValue());
		assertEquals(message.getMsg(), "\"get\" \" \" \"this is the \"\"key\"\"\" \"this is the\"\"\"\" value\"");
	}
	

	public void testConvertToBytes(){
		MessageType message = new MessageType("get"," ","key"," ");
		assertNull(message.error);
		byte[] bytes = message.getMsgBytes();
		Assert.assertArrayEquals(new byte[]{34,103,101,116,34,32,34,32,34,32,34,107,101,121,34,32,34,32,34,10}, bytes);
	}
	
	public void testConstructFromBytesServerToClient(){
		byte[] bytes = new byte[]{34,112,117,116,34,32,34,32,34,32,34,107,101,121,34,32,34,118,97,108,117,101,34,10};
		MessageType message = new MessageType(bytes);
		assertNull(message.error);
		assertEquals("put",message.getHeader());
		assertEquals(" ",message.getStatus());
		assertEquals("key",message.getKey());
		assertEquals("value",message.getValue());
		assertEquals("\"put\" \" \" \"key\" \"value\"",message.getMsg());	
	}
	
	public void testParseWithQuotesComplex(){
		MessageType message = new MessageType(" "," "," "," ");
		message.parse("\"put\" \" \" \"key\" \"\"\"a\"\"\"\"bc\"\"\"");
		assertEquals("put",message.getHeader());
		assertEquals(" ",message.getStatus());
		assertEquals("key",message.getKey());
		assertEquals("\"a\"\"bc\"", message.getValue());
	}
	
	public void testEmptyFields(){
		String msg = "\"get\" \"\" \"key\" \"\"";
		MessageType message = new MessageType(msg.getBytes());
		assertEquals(message.getHeader(), "get");
		assertEquals(message.getStatus(), "");
		assertEquals(message.getKey(), "key");
		assertEquals(message.getValue(), "");
	}
}
