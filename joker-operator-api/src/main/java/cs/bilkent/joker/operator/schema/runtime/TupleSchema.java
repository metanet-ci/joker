package cs.bilkent.joker.operator.schema.runtime;

import java.util.List;

import cs.bilkent.joker.operator.Tuple;

/**
 * Specifies the fields present in {@link Tuple} objects
 */
public interface TupleSchema
{

    int FIELD_NOT_FOUND = -1;

    /**
     * Returns number of fields specified in the schema for the tuple
     *
     * @return number of fields specified in the schema for the tuple
     */
    int getFieldCount ();

    /**
     * Returns list of fields specified in the schema for the tuple
     *
     * @return list of fields specified in the schema for the tuple
     */
    List<RuntimeSchemaField> getFields ();

    /**
     * Returns index of the requested field in the schema
     *
     * @param fieldName
     *         field name to get the index
     *
     * @return index of the requested field in the schema
     */
    int getFieldIndex ( String fieldName );

    /**
     * Returns the field present in the index of the schema
     *
     * @param fieldIndex
     *         index to get the field
     *
     * @return the field present in the index of the schema
     */
    String getFieldAt ( int fieldIndex );

}
