package cs.bilkent.joker.engine.kvstore.impl;

import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import static cs.bilkent.joker.engine.partition.PartitionUtil.getPartitionId;
import cs.bilkent.joker.engine.partition.impl.PartitionKeyExtractor1;
import cs.bilkent.joker.operator.Tuple;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PartitionedKVStoreContextRebalancingTest
{

    private static final String OPERATOR_ID = "op1";

    private static final int[] PARTITION_DISTRIBUTION = new int[] { 0, 0, 0, 0, 0, 1, 1, 1, 1, 1 };

    private static final int REPLICA_INDEX = 0;

    private static final int[] OWNED_PARTITIONS = new int[] { 0, 1, 2, 3, 4 };

    private static final int[] NON_OWNED_PARTITIONS = new int[] { 5, 6, 7, 8, 9 };

    private static final int PARTITION_COUNT = PARTITION_DISTRIBUTION.length;

    private static final String PARTITION_KEY_FIELD = "key";

    private static final PartitionKeyExtractor1 EXTRACTOR = new PartitionKeyExtractor1( singletonList( PARTITION_KEY_FIELD ) );


    private final KVStoreContainer[] kvStoreContainers = new KVStoreContainer[ PARTITION_COUNT ];

    private final Set<Object> keys = new HashSet<>();

    private PartitionedKVStoreContext kvStoreContext;

    @Before
    public void init ()
    {
        for ( int partitionId = 0; partitionId < PARTITION_COUNT; partitionId++ )
        {
            kvStoreContainers[ partitionId ] = new KVStoreContainer( partitionId );
        }

        kvStoreContext = new PartitionedKVStoreContext( OPERATOR_ID, REPLICA_INDEX, kvStoreContainers, PARTITION_DISTRIBUTION );
    }

    @Test
    public void shouldOwnPartitions ()
    {
        final int nonOwnedPartitionId = NON_OWNED_PARTITIONS[ 0 ];
        final Tuple tuple = generateTuple( nonOwnedPartitionId, keys );
        final KVStoreContainer partitionToOwn = kvStoreContainers[ nonOwnedPartitionId ];
        partitionToOwn.getOrCreateKVStore( EXTRACTOR.getPartitionKey( tuple ) ).set( "key", "val" );

        kvStoreContext.ownPartitions( new KVStoreContainer[] { partitionToOwn } );

        assertEquals( "val", kvStoreContext.getKVStore( EXTRACTOR.getPartitionKey( tuple ) ).get( "key" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldNotOwnAlreadyOwnedPartitions ()
    {
        kvStoreContext.ownPartitions( new KVStoreContainer[] { kvStoreContainers[ OWNED_PARTITIONS[ 0 ] ] } );
    }

    @Test
    public void shouldLeavePartitions ()
    {
        final KVStoreContainer[] left = kvStoreContext.leavePartitions( OWNED_PARTITIONS );
        assertEquals( left.length, OWNED_PARTITIONS.length );
        for ( KVStoreContainer partition : left )
        {
            boolean found = false;
            for ( int partitionId : OWNED_PARTITIONS )
            {
                if ( partitionId == partition.getPartitionId() )
                {
                    found = true;
                    break;
                }
            }

            assertTrue( found );
        }

        for ( int partitionId : OWNED_PARTITIONS )
        {
            try
            {
                kvStoreContext.getKVStore( EXTRACTOR.getPartitionKey( generateTuple( partitionId, keys ) ) );
                fail();
            }
            catch ( NullPointerException expected )
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
