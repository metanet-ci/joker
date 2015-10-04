package cs.bilkent.zanza.operator;

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
