package cs.bilkent.joker.engine.flow;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import cs.bilkent.joker.flow.FlowDef;
import cs.bilkent.joker.operator.OperatorDef;
import cs.bilkent.joker.operator.schema.runtime.PortRuntimeSchema;
import cs.bilkent.joker.operator.schema.runtime.RuntimeSchemaField;
import cs.bilkent.joker.operator.spec.OperatorType;
import static cs.bilkent.joker.operator.spec.OperatorType.PARTITIONED_STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATEFUL;
import static cs.bilkent.joker.operator.spec.OperatorType.STATELESS;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toList;

/**
 * Represents a part of the {@link FlowDef}, which consists of a chain of operators. Each region is monitored and scaled independently.
 * A region can be {@link OperatorType#STATELESS}, {@link OperatorType#STATEFUL} or {@link OperatorType#PARTITIONED_STATEFUL}.
 * <p>
 * - A {@link OperatorType#STATELESS} region can contain only {@link OperatorType#STATELESS} operators.
 * - A {@link OperatorType#STATEFUL} region can contain {@link OperatorType#STATEFUL} and {@link OperatorType#STATELESS} operators.
 * - A {@link OperatorType#PARTITIONED_STATEFUL} region can contain {@link OperatorType#PARTITIONED_STATEFUL} and
 * {@link OperatorType#STATELESS} operators.
 * <p>
 * If a region is {@link OperatorType#PARTITIONED_STATEFUL}, partition field names of first {@link OperatorType#PARTITIONED_STATEFUL}
 * operator in the region are used for data parallelism.
 */
public class RegionDef
{

    private static final Map<OperatorType, Set<OperatorType>> REGION_OPERATOR_TYPES;

    static
    {
        final Map<OperatorType, Set<OperatorType>> m = new HashMap<>();
        m.put( STATELESS, EnumSet.of( STATELESS ) );
        m.put( PARTITIONED_STATEFUL, EnumSet.of( PARTITIONED_STATEFUL, STATELESS ) );
        m.put( STATEFUL, EnumSet.of( STATEFUL, STATELESS ) );
        REGION_OPERATOR_TYPES = unmodifiableMap( m );
    }

    private final int regionId;

    private final OperatorType regionType;

    private final List<String> partitionFieldNames;

    private final List<OperatorDef> operators;

    public RegionDef ( final int regionId,
                       final OperatorType regionType,
                       final List<String> partitionFieldNames,
                       final List<OperatorDef> operators )
    {
        checkArgument( regionType != null, "regionId=%s region type cannot be null", regionId );
        checkArgument( partitionFieldNames != null,
                       "regionId=%s partition field names cannot be null! Use an empty list instead.",
                       regionId );
        checkArgument( operators != null && operators.size() > 0, "regionId=%s no operators!", regionId );
        checkOperatorTypes( regionId, regionType, operators );
        checkPartitionFieldNames( regionId, regionType, partitionFieldNames, operators );
        this.regionId = regionId;
        this.regionType = regionType;
        this.partitionFieldNames = unmodifiableList( new ArrayList<>( partitionFieldNames ) );
        this.operators = unmodifiableList( new ArrayList<>( operators ) );
    }

    private void checkOperatorTypes ( final int regionId, final OperatorType regionType, final List<OperatorDef> operators )
    {
        final Set<OperatorType> allowedOperatorTypes = REGION_OPERATOR_TYPES.get( regionType );
        boolean statelessOperatorFound = false, statefulOperatorFound = false, partitionedStatefulOperatorFound = false;
        for ( OperatorDef operator : operators )
        {
            statelessOperatorFound |= ( operator.getOperatorType() == STATELESS );
            statefulOperatorFound |= ( operator.getOperatorType() == STATEFUL );
            partitionedStatefulOperatorFound |= ( operator.getOperatorType() == PARTITIONED_STATEFUL );
            checkArgument( allowedOperatorTypes.contains( operator.getOperatorType() ),
                           "regionId=%s %s Operator: %s is not allowed in %s region. Allowed operator types: %s",
                           regionId,
                           operator.getOperatorType(),
                           operator.getId(),
                           regionType,
                           allowedOperatorTypes );
        }

        checkArgument( regionType != STATELESS || statelessOperatorFound,
                       "regionId=%s No %s operator in %s region!",
                       regionId,
                       STATELESS,
                       STATELESS );
        checkArgument( regionType != STATEFUL || statefulOperatorFound,
                       "regionId=%s No %s operator in %s region!",
                       regionId,
                       STATEFUL,
                       STATEFUL );
        checkArgument( regionType != PARTITIONED_STATEFUL || partitionedStatefulOperatorFound,
                       "regionId=%s No %s operator in %s region!",
                       regionId,
                       PARTITIONED_STATEFUL,
                       PARTITIONED_STATEFUL );
    }

    private void checkPartitionFieldNames ( final int regionId,
                                            final OperatorType regionType,
                                            final List<String> partitionFieldNames,
                                            final List<OperatorDef> operators )
    {
        checkArgument( regionType == PARTITIONED_STATEFUL ? partitionFieldNames.size() > 0 : partitionFieldNames.isEmpty(),
                       "invalid partition field names! regionId=%s regionType: %s partition field names: %s",
                       regionId,
                       regionType,
                       partitionFieldNames );

        if ( regionType != PARTITIONED_STATEFUL )
        {
            return;
        }

        final List<RuntimeSchemaField> partitionFields = new ArrayList<>();
        for ( OperatorDef operator : operators )
        {
            if ( operator.getOperatorType() == PARTITIONED_STATEFUL )
            {
                final List<String> operatorPartitionFieldNames = operator.getPartitionFieldNames();
                checkArgument( partitionFieldNames.equals( operatorPartitionFieldNames ) );
                final PortRuntimeSchema inputSchema = operator.getSchema().getInputSchema( 0 );
                for ( String partitionFieldName : partitionFieldNames )
                {
                    partitionFields.add( inputSchema.getField( partitionFieldName ) );
                }

                break;
            }
        }

        for ( OperatorDef operator : operators )
        {
            for ( int portIndex = 0; portIndex < operator.getInputPortCount(); portIndex++ )
            {
                final PortRuntimeSchema inputSchema = operator.getSchema().getInputSchema( portIndex );
                for ( RuntimeSchemaField partitionField : partitionFields )
                {
                    final RuntimeSchemaField field = inputSchema.getField( partitionField.getName() );
                    checkArgument( field != null,
                                   "partition field %s not found in schema of port index: %s of operator: %s",
                                   partitionField.getName(),
                                   portIndex,
                                   operator.getId() );
                    checkArgument( field.isCompatibleWith( partitionField ),
                                   "partition field %s is not compatible with field %s in schema of port index: %s of operator: %s",
                                   partitionField,
                                   field,
                                   portIndex,
                                   operator.getId() );
                }
            }
        }
    }

    /**
     * Returns index of the given operator if it is present in the region, -1 otherwise
     *
     * @param operator
     *         to get the index in the region
     *
     * @return index of the given operator if it is present in the region, -1 otherwise
     */
    public int indexOf ( final OperatorDef operator )
    {
        checkArgument( operator != null );
        return operators.indexOf( operator );
    }

    /**
     * Returns id of the region
     *
     * @return id of the region
     */
    public int getRegionId ()
    {
        return regionId;
    }

    /**
     * Returns type of the region
     *
     * @return type of the region
     */
    public OperatorType getRegionType ()
    {
        return regionType;
    }

    /**
     * Returns partition field names of the region, if the region is {@link OperatorType#PARTITIONED_STATEFUL}, empty list otherwise.
     *
     * @return partition field names of the region, if the region is {@link OperatorType#PARTITIONED_STATEFUL}, empty list otherwise.
     */
    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    public int getForwardedKeySize ()
    {
        return partitionFieldNames.size();
    }

    /**
     * Returns the operator with the given index in the region
     *
     * @param index
     *         in the region
     *
     * @return the operator with the given index in the region
     */
    public OperatorDef getOperator ( final int index )
    {
        return operators.get( index );
    }

    /**
     * Returns list of the operators present in the region
     *
     * @return list of the operators present in the region
     */
    public List<OperatorDef> getOperators ()
    {
        return operators;
    }

    /**
     * Returns number of operators present in the region
     *
     * @return number of operators present in the region
     */
    public int getOperatorCount ()
    {
        return operators.size();
    }

    /**
     * Returns true if the region contains a single operator with no input ports
     *
     * @return true if the region contains a single operator with no input ports
     */
    public boolean isSource ()
    {
        return getOperatorCount() == 1 && getOperator( 0 ).getInputPortCount() == 0;
    }

    @Override
    public boolean equals ( final Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        final RegionDef regionDef = (RegionDef) o;

        return regionId == regionDef.regionId && regionType == regionDef.regionType
               && partitionFieldNames.equals( regionDef.partitionFieldNames ) && operators.equals( regionDef.operators );
    }

    @Override
    public int hashCode ()
    {
        int result = regionId;
        result = 31 * result + regionType.hashCode();
        result = 31 * result + partitionFieldNames.hashCode();
        result = 31 * result + operators.hashCode();
        return result;
    }

    @Override
    public String toString ()
    {
        return "RegionDef{" + "regionId=" + regionId + ", regionType=" + regionType + ", partitionFieldNames=" + partitionFieldNames
               + ", operators=" + operators.stream().map( OperatorDef::getId ).collect( toList() ) + '}';
    }

}
