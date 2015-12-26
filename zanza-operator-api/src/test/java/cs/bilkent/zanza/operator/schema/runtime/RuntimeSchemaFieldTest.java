package cs.bilkent.zanza.operator.schema.runtime;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class RuntimeSchemaFieldTest
{

    @Test
    public void shouldBeCompatibleWithSuperclass ()
    {
        final RuntimeSchemaField field1 = new RuntimeSchemaField( "field1", Number.class );
        final RuntimeSchemaField field2 = new RuntimeSchemaField( "field1", Double.class );

        assertTrue( field1.isCompatibleWith( field2 ) );
        assertFalse( field2.isCompatibleWith( field1 ) );
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
