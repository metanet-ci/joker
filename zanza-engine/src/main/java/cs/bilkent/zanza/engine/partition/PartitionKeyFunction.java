package cs.bilkent.zanza.engine.partition;

import cs.bilkent.zanza.operator.Tuple;

public interface PartitionKeyFunction
{

    Object getPartitionKey ( final Tuple tuple );

    int getPartitionHash ( final Tuple tuple );

}
