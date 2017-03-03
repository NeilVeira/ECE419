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
		assertEquals(md.toString(), "-134847710425560069445028245650825152028 localhost 50000,-2960810945850692900445322256017604746 127.0.0.01 50001");
	}
	
	public void testRemoveServerPresent() {
		md.addServer(md.new Server("localhost",50000));
		md.removeServer(md.new Server("localhost",50000)); 
		assertEquals(md.toString(), "");
	}
	
	public void testRemoveServerNotPresent() {
		md.addServer(md.new Server("localhost",50000));
		md.removeServer(md.new Server("localhost",50001)); 
		assertEquals(md.toString(), "-134847710425560069445028245650825152028 localhost 50000");
	}
	
	public void testGetResponsible() {
		md.addServer(md.new Server("localhost",50000));
		md.addServer(md.new Server("localhost",50001));
		md.addServer(md.new Server("localhost",50002));
		md.addServer(md.new Server("127.0.0.1",1234));
		Metadata.Server responsible = md.getResponsible("key");
		assertEquals(responsible.ipAddress, "localhost");
		assertEquals(responsible.port, 50001);
	}
	
	public void testGetResponsibleWrapAround() {
		//in this case the key hash is larger than the largest server hash
		md.addServer(md.new Server("localhost",50000));
		md.addServer(md.new Server("localhost",50001));
		md.addServer(md.new Server("localhost",50002));
		md.addServer(md.new Server("127.0.0.1",1234));
		Metadata.Server responsible = md.getResponsible("17");
		assertEquals(responsible.ipAddress, "localhost");
		assertEquals(responsible.port, 50000);
	}
	
	public void testConstructFromString() {
		String data = "-134847710425560069445028245650825152028 localhost 50000,-93864682652215908080847256054918673801 localhost 50002,36187173043867385737752624992350489329 127.0.0.1 1234,136415732930669195156142751695833227657 localhost 50001";
		Metadata md2 = new Metadata(data);
		assertEquals(md2.toString(), data);
		Metadata.Server responsible = md2.getResponsible("17");
		assertEquals(responsible.ipAddress, "localhost");
		assertEquals(responsible.port, 50000);
	}
	
}