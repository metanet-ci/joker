package cs.bilkent.zanza.operator;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TupleTest extends AbstractFieldsTest
{
	private Tuple tuple;

	@Before
	public void init()
	{
		tuple = getFields();
	}

	@Override
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

    @Test
    public void testTupleEquality1 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality2 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality3 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality4 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality5 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality6 ()
    {
        final Tuple tuple1 = new Tuple( true );
        final Tuple tuple2 = new Tuple( false );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality7 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k1", "v1" );
        tuple2.set( "k2", "v2" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }

    @Test
    public void testTupleEquality8 ()
    {
        final Tuple tuple1 = new Tuple( false );
        final Tuple tuple2 = new Tuple( true );

        tuple1.set( "k1", "v1" );
        tuple1.set( "k2", "v2" );

        tuple2.set( "k2", "v2" );
        tuple2.set( "k1", "v1" );

        assertThat( tuple1, equalTo( tuple2 ) );
        assertThat( tuple1.hashCode(), equalTo( tuple2.hashCode() ) );
    }
}
