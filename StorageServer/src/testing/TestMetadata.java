package testing;

import org.junit.Test;
import org.junit.Assert;

import common.Metadata;
import java.math.BigInteger;

import junit.framework.TestCase;

public class TestMetadata extends TestCase {
	Metadata md;
	
	public void setUp() {
		this.md = new Metadata();
	}

	public void tearDown() {
	}
	
	public void testHash() {
		BigInteger hash = md.serverHash(md.new Server("localhost", 50002));
		assertEquals(hash.toString(), "-93864682652215908080847256054918673801");
	}
	
	public void testAddServer() {
		md.addServer(md.new Server("localhost",50000));
		md.addServer(md.new Server("127.0.0.01",50001));
		md.addServer(md.new Server("localhost",50000)); 
		//TODO: check correctness
	}
	
	public void testRemoveServerPresent() {
		md.addServer(md.new Server("localhost",50000));
		md.removeServer(md.new Server("localhost",50000));
		//TODO: check correctness
	}
	
	//TODO: test getResponsible
	
}