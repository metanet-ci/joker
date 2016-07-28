package cs.bilkent.zanza.operator.impl;

import java.util.List;

import org.junit.Test;

import cs.bilkent.testutils.ZanzaTest;
import cs.bilkent.zanza.operator.Tuple;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;


public class TuplesImplTest extends ZanzaTest
{
    private final TuplesImpl tuples = new TuplesImpl( 2 );

    @Test
    public void shouldAddTupleToDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        tuples.add( tuple );

        final List<Tuple> tuples = this.tuples.getTuples( 0 );
        assertThat( tuples, hasSize( 1 ) );
        assertThat( tuples, hasItem( tuple ) );
    }

    @Test
    public void shouldGetTuplesByDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        tuples.add( tuple );

        final List<Tuple> tuples = this.tuples.getTuplesByDefaultPort();
        assertThat( tuples, hasSize( 1 ) );
        assertThat( tuples, hasItem( tuple ) );
    }

    @Test
    public void shouldNotGetTuplesByDefaultPort ()
    {
        final Tuple tuple = new Tuple();

        final int portIndex = 1;
        tuples.add( portIndex, tuple );

        final List<Tuple> tuples = this.tuples.getTuples( portIndex );
        assertThat( tuples, hasSize( 1 ) );

        final List<Tuple> tuplesOnDefaultPart = this.tuples.getTuplesByDefaultPort();
        assertThat( tuplesOnDefaultPart, hasSize( 0 ) );
    }

    @Test
    public void shouldAddTuplesToMultiplePorts ()
    {
        final Tuple tuple = new Tuple();

        tuples.add( 0, tuple );
        tuples.add( 1, tuple );

        assertThat( tuples.getNonEmptyPortCount(), equalTo( 2 ) );

        final List<Tuple> tuples1 = tuples.getTuples( 0 );
        assertThat( tuples1, hasSize( 1 ) );
        assertThat( tuples1, hasItem( tuple ) );
        final List<Tuple> tuples2 = tuples.getTuples( 1 );
        assertThat( tuples2, hasSize( 1 ) );
        assertThat( tuples2, hasItem( tuple ) );
    }

}
