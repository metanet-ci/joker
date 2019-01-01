package cs.bilkent.joker.operator.schema.runtime;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import cs.bilkent.joker.operator.schema.annotation.PortSchema;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import static java.util.Collections.unmodifiableList;


/**
 * Runtime representation of {@link PortSchema}
 */
public final class PortRuntimeSchema implements TupleSchema
{

    private final List<RuntimeSchemaField> fields;

    private final TObjectIntMap<String> fieldIndices = new TObjectIntHashMap<>( 4, 0.75f, FIELD_NOT_FOUND );

    /**
     * Creates the {@code PortRuntimeSchema} using the given field definitions. Sorts the fields by field name.
     *
     * @param fields
     *         to be included in the created {@code PortRuntimeSchema}
     */
    public PortRuntimeSchema ( final List<RuntimeSchemaField> fields )
    {
        final List<RuntimeSchemaField> f = new ArrayList<>( fields );
        f.sort( Comparator.comparing( RuntimeSchemaField::getName ) );
        this.fields = unmodifiableList( f );
        for ( int i = 0; i < f.size(); i++ )
        {
            fieldIndices.put( f.get( i ).getName(), i );
        }
    }

    /**
     * Returns the number of fields in the port schema
     *
     * @return the number of fields in the port schema
     */
    @Override
    public int getFieldCount ()
    {
        return fields.size();
    }

    /**
     * Returns the fields in the port, sorted by field name
     *
     * @return the fields in the port, sorted by field name
     */
    @Override
    public List<RuntimeSchemaField> getFields ()
    {
        return fields;
    }

    /**
     * Returns index of the given field
     *
     * @param fieldName
     *         to get the index
     *
     * @return index of the given field
     */
    @Override
    public int getFieldIndex ( final String fieldName )
    {
        return fieldIndices.get( fieldName );
    }

    /**
     * Returns the field name at the given index
     *
     * @param fieldIndex
     *         to get the field name
     *
     * @return the field name at the given index
     */
    @Override
    public String getFieldAt ( final int fieldIndex )
    {
        return fields.get( fieldIndex ).getName();
    }

    /**
     * Returns the {@link RuntimeSchemaField} for the given field name
     *
     * @param fieldName
     *         to get the {@link RuntimeSchemaField} object
     *
     * @return the {@link RuntimeSchemaField} for the given field name
     */
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

    /**
     * Checks if all of the fields in the argument have compatible fields in this schema
     *
     * @param other
     *         schema to check
     *
     * @return true if all of the fields in the argument have compatible fields in this schema
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

    @Override
    public String toString ()
    {
        return "PortRuntimeSchema{" + fields + '}';
    }

}
