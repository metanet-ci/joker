package cs.bilkent.zanza.operator;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;

public class PortsToTuplesTest
{
	private final PortsToTuples portsToTuples = new PortsToTuples();

	@Test
	public void shouldAddTupleToDefaultPort()
	{
		final Tuple tuple = mock(Tuple.class);

		portsToTuples.add(tuple);

		assertThat(portsToTuples.getPortCount(), equalTo(1));
		assertThat(portsToTuples.getPorts(), equalTo(new int[] { Port.DEFAULT_PORT_INDEX }));

		final List<Tuple> tuples = portsToTuples.getTuples(Port.DEFAULT_PORT_INDEX);
		assertThat(tuples, hasSize(1));
		assertThat(tuples, hasItem(tuple));
	}

	@Test
	public void shouldGetTuplesByDefaultPort()
	{
		final Tuple tuple = mock(Tuple.class);

		portsToTuples.add(tuple);

		assertThat(portsToTuples.getPortCount(), equalTo(1));
		final List<Tuple> tuples = portsToTuples.getTuplesByDefaultPort();
		assertThat(tuples, hasSize(1));
		assertThat(tuples, hasItem(tuple));
	}

	@Test
	public void shouldNotGetTuplesByDefaultPort()
	{
		final Tuple tuple = mock(Tuple.class);

		final int portIndex = 1;
		portsToTuples.add(portIndex, tuple);

		assertThat(portsToTuples.getPortCount(), equalTo(portIndex));
		assertThat(portsToTuples.getPorts(), equalTo(new int[] { portIndex }));

		final List<Tuple> tuples = portsToTuples.getTuplesByDefaultPort();
		assertThat(tuples, hasSize(0));
	}

	@Test
	public void shouldAddTuplesToMultiplePorts()
	{
		final Tuple tuple = mock(Tuple.class);

		portsToTuples.add(0, tuple);
		portsToTuples.add(1, tuple);

		assertThat(portsToTuples.getPortCount(), equalTo(2));
		final int[] ports = portsToTuples.getPorts();
		assertThat(ports, equalTo(new int[] { 0, 1 }));

		final List<Tuple> tuples1 = portsToTuples.getTuples(0);
		assertThat(tuples1, hasSize(1));
		assertThat(tuples1, hasItem(tuple));
		final List<Tuple> tuples2 = portsToTuples.getTuples(1);
		assertThat(tuples2, hasSize(1));
		assertThat(tuples2, hasItem(tuple));
	}

}
