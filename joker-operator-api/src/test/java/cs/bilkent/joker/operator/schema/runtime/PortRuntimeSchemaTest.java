package cs.bilkent.joker.operator.schema.runtime;

import org.junit.Test;

import cs.bilkent.joker.test.AbstractJokerTest;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class PortRuntimeSchemaTest extends AbstractJokerTest
{

    private final PortRuntimeSchemaBuilder sourceSchemaBuilder = new PortRuntimeSchemaBuilder();

    private final PortRuntimeSchemaBuilder targetSchemaBuilder = new PortRuntimeSchemaBuilder();

    @Test
    public void shouldNotBeCompatibleWithMissingFields ()
    {
        targetSchemaBuilder.addField( "field1", Integer.class );

        final PortRuntimeSchema sourceSchema = sourceSchemaBuilder.build();
        final PortRuntimeSchema targetSchema = targetSchemaBuilder.build();

        assertFalse( sourceSchema.isCompatibleWith( targetSchema ) );
    }

    @Test
    public void shouldBeCompatibleWithSubclassOnSourceSchema ()
    {
        sourceSchemaBuilder.addField( "field1", Integer.class );
        targetSchemaBuilder.addField( "field1", Number.class );

        final PortRuntimeSchema sourceSchema = sourceSchemaBuilder.build();
        final PortRuntimeSchema targetSchema = targetSchemaBuilder.build();

        assertTrue( sourceSchema.isCompatibleWith( targetSchema ) );
    }

    @Test
    public void shouldNotBeCompatibleWithSuperclassOnSourceSchema ()
    {
        sourceSchemaBuilder.addField( "field1", Number.class );
        targetSchemaBuilder.addField( "field1", Integer.class );

        final PortRuntimeSchema sourceSchema = sourceSchemaBuilder.build();
        final PortRuntimeSchema targetSchema = targetSchemaBuilder.build();

        assertFalse( sourceSchema.isCompatibleWith( targetSchema ) );
    }

}
