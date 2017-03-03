package testing;

import org.junit.Test;
import org.junit.Assert;

import common.Metadata;
import java.math.BigInteger;

import junit.framework.TestCase;

public class TestMetadata extends TestCase {
	
	public void setUp() {
	}

	public void tearDown() {
	}
	
	public void testHash() {
		Metadata md = new Metadata();
		BigInteger hash = md.getHash("localhost", 50002);
		assertEquals(hash.toString(), "-93864682652215908080847256054918673801");
	}
	
}