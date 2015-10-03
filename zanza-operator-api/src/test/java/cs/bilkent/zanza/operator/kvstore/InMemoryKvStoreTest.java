package cs.bilkent.zanza.operator.kvstore;

import org.junit.Test;

import cs.bilkent.zanza.operator.AbstractFieldsTest;
import cs.bilkent.zanza.operator.Fields;
import cs.bilkent.zanza.operator.impl.kvstore.InMemoryKVStore;

public class InMemoryKVStoreTest extends AbstractFieldsTest
{

	protected Fields newFieldsInstance()
	{
		return new InMemoryKVStore();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldFailToClear()
	{
		getFields().clear();
	}

	@Test(expected = UnsupportedOperationException.class)
	public void shouldFailToGetSize()
	{
		getFields().size();
	}

}
