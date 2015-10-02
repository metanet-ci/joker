package cs.bilkent.zanza.operator.scheduling;

import cs.bilkent.zanza.operator.Port;
import uk.co.real_logic.agrona.collections.Int2IntHashMap;

import java.util.Map;

public class TupleAvailabilityBasedSchedulingStrategy implements SchedulingStrategy
{

	private final Int2IntHashMap tupleCountByPortIndex = new Int2IntHashMap(0);
	private final TupleAvailabilityType tupleAvailabilityType;

	public TupleAvailabilityBasedSchedulingStrategy()
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ANY;
		this.tupleCountByPortIndex.put(Port.DEFAULT_PORT_INDEX, 1);
	}

	public TupleAvailabilityBasedSchedulingStrategy(int portIndex, int tupleCount)
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ANY;
		tupleCountByPortIndex.put(portIndex, tupleCount);
	}

	public TupleAvailabilityBasedSchedulingStrategy(Map<Integer, Integer> tupleCountByPortIndex)
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ALL;
		this.tupleCountByPortIndex.putAll(tupleCountByPortIndex);
	}

	public TupleAvailabilityBasedSchedulingStrategy(Map<Integer, Integer> tupleCountByPortIndex,
			TupleAvailabilityType tupleAvailabilityType)
	{
		this.tupleAvailabilityType = tupleAvailabilityType;
		this.tupleCountByPortIndex.putAll(tupleCountByPortIndex);
	}

	public Map<Integer, Integer> getTupleCountByPortIndex()
	{
		return tupleCountByPortIndex;
	}

	public TupleAvailabilityType getTupleAvailabilityType()
	{
		return tupleAvailabilityType;
	}

	enum TupleAvailabilityType
	{
		AVAILABLE_ON_ALL,
		AVAILABLE_ON_ANY
	}

}
