package cs.bilkent.zanza.region;

import java.util.ArrayList;
import java.util.List;

import cs.bilkent.zanza.operator.spec.OperatorType;
import static java.util.Collections.unmodifiableList;

public class RegionDefinition
{

    private final OperatorType regionType;

    private final List<String> partitionFieldNames;

    private final List<String> operatorIds;

    public RegionDefinition ( final OperatorType regionType, final List<String> partitionFieldNames, final List<String> operatorIds )
    {
        this.regionType = regionType;
        this.partitionFieldNames = unmodifiableList( new ArrayList<>( partitionFieldNames ) );
        this.operatorIds = unmodifiableList( new ArrayList<>( operatorIds ) );
    }

    public OperatorType getRegionType ()
    {
        return regionType;
    }

    public List<String> getPartitionFieldNames ()
    {
        return partitionFieldNames;
    }

    public List<String> getOperatorIds ()
    {
        return operatorIds;
    }

}
