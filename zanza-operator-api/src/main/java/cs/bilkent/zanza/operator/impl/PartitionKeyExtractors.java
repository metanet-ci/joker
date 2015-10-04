package cs.bilkent.zanza.operator.impl;

import cs.bilkent.zanza.operator.PartitionKeyExtractor;

public final class PartitionKeyExtractors
{
    private PartitionKeyExtractors()
    {
    }

    public static PartitionKeyExtractor fieldAsPartitionKey(final String fieldName)
    {
        return (tuple) -> tuple.getObject(fieldName);
    }
}
