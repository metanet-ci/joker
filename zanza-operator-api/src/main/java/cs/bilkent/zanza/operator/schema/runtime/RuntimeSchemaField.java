package cs.bilkent.zanza.operator.schema.runtime;

import static com.google.common.base.Preconditions.checkArgument;

public class RuntimeSchemaField
{

    public final String name;

    public final Class<?> type;

    public RuntimeSchemaField ( final String name, final Class<?> type )
    {
        checkArgument( name != null, "name can't be null" );
        checkArgument( type != null, "type can't be null" );
        this.name = name;
        this.type = type;
    }

    /**
     * Checks if this field has same name with other field and its type is a sub-class of other field or not
     *
     * @param other
     *         field to check
     *
     * @return true if this field has same name with other field and its type is a sub-class of other field
     */
    public boolean isCompatibleWith ( final RuntimeSchemaField other )
    {
        return name.equals( other.name ) && other.type.isAssignableFrom( this.type );
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final RuntimeSchemaField that = (RuntimeSchemaField) o;

        if ( !name.equals( that.name ) )
        {
            return false;
        }
        return type.equals( that.type );

    }

    @Override
    public int hashCode ()
    {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

}
