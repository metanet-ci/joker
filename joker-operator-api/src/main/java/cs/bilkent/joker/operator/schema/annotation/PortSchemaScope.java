package cs.bilkent.joker.operator.schema.annotation;


import cs.bilkent.joker.flow.FlowDef;


/**
 * Defines scope of the fields given in a {@link PortSchema}
 */
public enum PortSchemaScope
{

    /**
     * All of the fields in the port schema is defined in design time and no more
     * fields can be added to the port schema while composing the {@link FlowDef} in the runtime.
     */
    EXACT_FIELD_SET,

    /**
     * There can be some fields defined in design time for the port schema.
     * Additionally, more fields can be added to the port schema while composing the {@link FlowDef} in the runtime.
     */
    EXTENDABLE_FIELD_SET

}
