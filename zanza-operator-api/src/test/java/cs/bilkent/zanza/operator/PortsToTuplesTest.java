package cs.bilkent.zanza.operator;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import cs.bilkent.zanza.operator.PortsToTuples.PortToTuples;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;


public class PortsToTuplesTest
{
    private final PortsToTuples portsToTuples = new PortsToTuples();

    @Test
    public void shouldAddTupleToDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        portsToTuples.add( tuple );

        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();
        assertThat( portToTuplesList, hasSize( 1 ) );
        final PortToTuples portToTuples = portToTuplesList.get( 0 );
        assertThat( portToTuples.getPortIndex(), equalTo( 0 ) );
        assertThat( portToTuples.getTuples(), hasSize( 1 ) );

        final List<Tuple> tuples = portsToTuples.getTuples( 0 );
        assertThat( tuples, hasSize( 1 ) );
        assertThat( tuples, hasItem( tuple ) );
    }

    @Test
    public void shouldGetTuplesByDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        portsToTuples.add( tuple );

        assertThat( portsToTuples.getPortCount(), equalTo( 1 ) );
        final List<Tuple> tuples = portsToTuples.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( 1 ) );
        assertThat( tuples, hasItem( tuple ) );
    }

    @Test
    public void shouldNotGetTuplesByDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        final int portIndex = 1;
        portsToTuples.add( portIndex, tuple );

        assertThat( portsToTuples.getPortCount(), equalTo( portIndex ) );
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();
        assertThat( portToTuplesList, hasSize( 1 ) );
        final PortToTuples portToTuples = portToTuplesList.get( 0 );
        assertThat( portToTuples.getPortIndex(), equalTo( portIndex ) );
        assertThat( portToTuples.getTuples(), hasSize( 1 ) );

        final List<Tuple> tuples = portsToTuples.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( 0 ) );
    }

    @Test
    public void shouldAddTuplesToMultiplePorts ()
    {
        final Tuple tuple = new Tuple();

        portsToTuples.add( 0, tuple );
        portsToTuples.add( 1, tuple );

        assertThat( portsToTuples.getPortCount(), equalTo( 2 ) );
        final List<PortToTuples> portToTuplesList = portsToTuples.getPortToTuplesList();
        final PortToTuples portToTuples1 = portToTuplesList.get( 0 );
        final PortToTuples portToTuples2 = portToTuplesList.get( 1 );
        assertThat( portToTuples1.getPortIndex(), equalTo( 0 ) );
        assertThat( portToTuples1.getTuples(), hasSize( 1 ) );
        assertThat( portToTuples2.getPortIndex(), equalTo( 1 ) );
        assertThat( portToTuples2.getTuples(), hasSize( 1 ) );

        final List<Tuple> tuples1 = portsToTuples.getTuples( 0 );
        assertThat( tuples1, hasSize( 1 ) );
        assertThat( tuples1, hasItem( tuple ) );
        final List<Tuple> tuples2 = portsToTuples.getTuples( 1 );
        assertThat( tuples2, hasSize( 1 ) );
        assertThat( tuples2, hasItem( tuple ) );
    }

    @Test
    public void shouldCollectAllTuplesToDefaultPort ()
    {
        final List<Tuple> tuples = new ArrayList<>();
        tuples.add( new Tuple( "k1", "v1" ) );
        tuples.add( new Tuple( "k2", "v2" ) );
        tuples.add( new Tuple( "k3", "v3" ) );

        final PortsToTuples output = tuples.stream().collect( PortsToTuples.COLLECT_TO_DEFAULT_PORT );
        assertThat( output.getPortCount(), equalTo( 1 ) );
        assertThat( output.getTuplesByDefaultPort(), equalTo( tuples ) );
    }

    @Test
    public void shouldCollectAllTuplesToGivenPort ()
    {
        final List<Tuple> tuples = new ArrayList<>();
        tuples.add( new Tuple( "k1", "v1" ) );
        tuples.add( new Tuple( "k2", "v2" ) );
        tuples.add( new Tuple( "k3", "v3" ) );

        final int port = 5;
        final PortsToTuples output = tuples.stream().collect( PortsToTuples.collectToPort( port ) );
        assertThat( output.getPortCount(), equalTo( 1 ) );
        assertThat( output.getTuples( port ), equalTo( tuples ) );
    }

    @Test
    public void shouldCollectAllTuplesToGivenPortsToTuples ()
    {
        final List<Tuple> tuples = new ArrayList<>();
        tuples.add( new Tuple( "k1", "v1" ) );
        tuples.add( new Tuple( "k2", "v2" ) );
        tuples.add( new Tuple( "k3", "v3" ) );

        final int port = 5;
        final PortsToTuples target = new PortsToTuples();
        final PortsToTuples output = tuples.stream().collect( PortsToTuples.collectTo( target, port ) );
        assertTrue( target == output );
        assertThat( target.getPortCount(), equalTo( 1 ) );
        assertThat( target.getTuples( port ), equalTo( tuples ) );
    }

}
