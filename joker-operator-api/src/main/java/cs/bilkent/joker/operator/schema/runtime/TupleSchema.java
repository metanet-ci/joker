package cs.bilkent.joker.operator.schema.runtime;

import java.util.List;

public interface TupleSchema
{

    int FIELD_NOT_FOUND = -1;

    int getFieldCount ();

    List<RuntimeSchemaField> getFields ();

    int getFieldIndex ( String fieldName );

    String getFieldAt ( int fieldIndex );

}
