package cs.bilkent.zanza.operator.schema.annotation;


import cs.bilkent.zanza.flow.FlowDefinition;


/**
 * Enum to define scope of the fields given in a {@link PortSchema} instance.
 */
public enum PortSchemaScope
{

    /**
     * All of the {@link SchemaField} instances for the port schema is defined in design time and no more
     * fields can be added to the port schema while composing the {@link FlowDefinition}.
     */
    EXACT_FIELD_SET,

    /**
     * There might be some fields defined in design time for the port schema. Additionally, there might be
     * some additional fields that can be added to the port schema while composing the {@link FlowDefinition}.
     */
    BASE_FIELD_SET

}
