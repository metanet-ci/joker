package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import static gnu.trove.impl.Constants.DEFAULT_CAPACITY;
import static gnu.trove.impl.Constants.DEFAULT_LOAD_FACTOR;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import static java.util.Collections.unmodifiableList;


/**
 * Runtime representation of {@link PortSchema}
 */
public final class PortRuntimeSchema implements TupleSchema
{

    private final List<RuntimeSchemaField> fields;

    private final TObjectIntMap<String> fieldIndices = new TObjectIntHashMap<>( DEFAULT_CAPACITY, DEFAULT_LOAD_FACTOR, FIELD_NOT_FOUND );

    public PortRuntimeSchema ( final List<RuntimeSchemaField> fields )
    {
        final ArrayList<RuntimeSchemaField> f = new ArrayList<>( fields );
        Collections.sort( f, ( o1, o2 ) -> o1.getName().compareTo( o2.getName() ) );
        this.fields = unmodifiableList( f );
        for ( int i = 0; i < f.size(); i++ )
        {
            fieldIndices.put( f.get( i ).getName(), i );
        }
    }

    @Override
    public int getFieldCount ()
    {
        return fields.size();
    }

    @Override
    public List<RuntimeSchemaField> getFields ()
    {
        return fields;
    }

    @Override
    public int getFieldIndex ( final String fieldName )
    {
        return fieldIndices.get( fieldName );
    }

    @Override
    public String getFieldAt ( final int fieldIndex )
    {
        return fields.get( fieldIndex ).getName();
    }

    /**
     * Checks if all of the fields in the other schema has a corresponding compatible field in this schema or not
     *
     * @param other
     *         schema to check
     *
     * @return true if all of the fields in the other schema has a corresponding compatible field in this schema
     */
    public boolean isCompatibleWith ( final PortRuntimeSchema other )
    {
        for ( RuntimeSchemaField otherField : other.getFields() )
        {
            boolean match = false;
            for ( RuntimeSchemaField thisField : this.fields )
            {
                if ( thisField.isCompatibleWith( otherField ) )
                {
                    match = true;
                    break;
                }
            }

            if ( !match )
            {
                return false;
            }
        }

        return true;
    }

    public RuntimeSchemaField getField ( final String fieldName )
    {
        for ( RuntimeSchemaField thisField : this.fields )
        {
            if ( thisField.name.equals( fieldName ) )
            {
                return thisField;
            }
        }

        return null;
    }

    @Override
    public String toString ()
    {
        return "PortRuntimeSchema{" + fields + '}';
    }

}
