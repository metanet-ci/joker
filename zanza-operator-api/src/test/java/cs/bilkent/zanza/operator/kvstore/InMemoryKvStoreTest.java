package cs.bilkent.zanza.operator.kvstore;

import org.junit.Test;

import cs.bilkent.zanza.operator.AbstractFieldsTest;
import cs.bilkent.zanza.operator.Fields;


public class InMemoryKvStoreTest extends AbstractFieldsTest
{
	@Override
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
