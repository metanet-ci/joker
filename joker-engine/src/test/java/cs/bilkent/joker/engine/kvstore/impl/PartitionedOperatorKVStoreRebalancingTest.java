package cs.bilkent.joker.engine.kvstore.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.operator.Tuple;
import cs.bilkent.joker.test.AbstractJokerTest;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionedOperatorKVStoreRebalancingTest extends AbstractJokerTest
{

    private static final String OPERATOR_ID = "op1";

    private static final int[] PARTITION_DISTRIBUTION = new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 };

    private static final int REPLICA_INDEX = 0;

    private static final List<Integer> ACQUIRED_PARTITIONS = asList( 0, 1, 2, 3, 4 );

    private static final List<Integer> NON_ACQUIRED_PARTITIONS = asList( 5, 6, 7, 8, 9 );

    private static final int PARTITION_COUNT = PARTITION_DISTRIBUTION.length;

    private static final String PARTITION_KEY_FIELD = "key";

    private static final PartitionKeyExtractor1 EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );


    private final KVStoreContainer[] kvStoreContainers = new KVStoreContainer[ PARTITION_COUNT ];

    private final Set<Object> keys = new HashSet<>();

    private PartitionedOperatorKVStore operatorKVStore;

    @Before
    public void init ()
    {
        for ( int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++ )
        {
            kvStoreContainers[ partitionId ] = new KVStoreContainer( partitionId );
        }

        operatorKVStore = new PartitionedOperatorKVStore( OPERATOR_ID, REPLICA_INDEX, kvStoreContainers, PARTITION_DISTRIBUTION );
    }

    @Test
    public void shouldAcquirePartitions ()
    {
        final int nonAcquiredPartitionId = NON_ACQUIRED_PARTITIONS.get( 0 );
        final Tuple tuple = generateTuple( nonAcquiredPartitionId, keys );
        final KVStoreContainer partitionToAcquire = kvStoreContainers[ nonAcquiredPartitionId ];
        partitionToAcquire.getOrCreateKVStore( EXTRACTOR.getPartitionKey( tuple ) ).set( "key", "val" );

        operatorKVStore.acquirePartitions( singletonList( partitionToAcquire ) );

        assertEquals( "val", operatorKVStore.getKVStore( EXTRACTOR.getPartitionKey( tuple ) ).get( "key" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotAcquireAlreadyAcquiredPartitions ()
    {
        operatorKVStore.acquirePartitions( singletonList( kvStoreContainers[ ACQUIRED_PARTITIONS.get( 0 ) ] ) );
    }

    @Test
    public void shouldReleasePartitions ()
    {
        final List<KVStoreContainer> released = operatorKVStore.releasePartitions( ACQUIRED_PARTITIONS );
        assertEquals( released.size(), ACQUIRED_PARTITIONS.size() );
        for ( KVStoreContainer partition : released )
        {
            boolean found = false;
            for ( int partitionId : ACQUIRED_PARTITIONS )
            {
                if ( partitionId == partition.getPartitionId() )
                {
                    found = true;
                    break;
                }
            }

            assertTrue( found );
        }

        for ( int partitionId : ACQUIRED_PARTITIONS )
        {
            try
            {
                operatorKVStore.getKVStore( EXTRACTOR.getPartitionKey( generateTuple( partitionId, keys ) ) );
                fail();
            }
            catch ( NullPointerException ignored )
            {

            }
        }
    }

    private Tuple generateTuple ( final int partitionId, final Set<Object> existingKeys )
    {
        final Tuple tuple = new Tuple();
        int i = 0;
        while ( true )
        {
            if ( existingKeys.contains( i ) )
            {
                i++;
                continue;
            }

            tuple.set( PARTITION_KEY_FIELD, i );
            final int partitionHash = EXTRACTOR.getPartitionHash( tuple );

            if ( getPartitionId( partitionHash, PARTITION_COUNT ) == partitionId )
            {
                keys.add( i );
                return tuple;
            }

            i++;
        }
    }

}
