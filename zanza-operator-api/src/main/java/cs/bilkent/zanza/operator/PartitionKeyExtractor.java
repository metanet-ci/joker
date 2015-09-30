package cs.bilkent.zanza.operator;

/**
 *
 */
@FunctionalInterface
public interface PartitionKeyExtractor {

    Object getPartitionKey(Tuple tuple);

}
