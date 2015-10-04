package cs.bilkent.zanza.operator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.junit.Before;
import org.junit.Test;

public class TupleTest extends AbstractFieldsTest
{

	private Tuple tuple;

	@Before
	public void init()
	{
		tuple = getFields();
	}

	protected Fields newFieldsInstance()
	{
		return new Tuple();
	}

	@Test
	public void shouldClear()
	{
		tuple.set("field", "value");
		tuple.clear();
		assertThat(tuple.size(), equalTo(0));
	}

	@Test
	public void shouldGetSize()
	{
		tuple.set("field", "value");
		assertThat(tuple.size(), equalTo(1));
	}
}
