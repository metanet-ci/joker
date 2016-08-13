package cs.bilkent.joker.operator.schema.runtime;

import org.junit.Test;

import cs.bilkent.joker.testutils.AbstractJokerTest;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class RuntimeSchemaFieldTest extends AbstractJokerTest
{

    @Test
    public void shouldNotBeCompatibleWithSuperclass ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", Number.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field1", Double.class );

        assertFalse( field1.isCompatibleWith( field2 ) );
    }

    @Test
    public void shouldBeCompatibleWithSubclass ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", Double.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field1", Number.class );

        assertTrue( field1.isCompatibleWith( field2 ) );
    }

    @Test
    public void shouldNotBeCompatibleWithDifferentName ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", Double.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field2", Double.class );

        assertFalse( field1.isCompatibleWith( field2 ) );
        assertFalse( field2.isCompatibleWith( field1 ) );
    }

}
