package cs.bilkent.zanza.operator.impl.scheduling;

import java.util.Map;

import uk.co.real_logic.agrona.collections.Int2IntHashMap;
import cs.bilkent.zanza.operator.Port;
import cs.bilkent.zanza.operator.scheduling.SchedulingStrategy;

public class ScheduleWhenTuplesAvailable implements SchedulingStrategy
{

	private final Int2IntHashMap tupleCountByPortIndex = new Int2IntHashMap(0);
	private final TupleAvailabilityType tupleAvailabilityType;

	public ScheduleWhenTuplesAvailable()
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ANY;
		this.tupleCountByPortIndex.put(Port.DEFAULT_PORT_INDEX, 1);
	}

	public ScheduleWhenTuplesAvailable(int portIndex, int tupleCount)
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ANY;
		tupleCountByPortIndex.put(portIndex, tupleCount);
	}

	public ScheduleWhenTuplesAvailable(Map<Integer, Integer> tupleCountByPortIndex)
	{
		this.tupleAvailabilityType = TupleAvailabilityType.AVAILABLE_ON_ALL;
		this.tupleCountByPortIndex.putAll(tupleCountByPortIndex);
	}

	public ScheduleWhenTuplesAvailable(Map<Integer, Integer> tupleCountByPortIndex, TupleAvailabilityType tupleAvailabilityType)
	{
		this.tupleAvailabilityType = tupleAvailabilityType;
		this.tupleCountByPortIndex.putAll(tupleCountByPortIndex);
	}

	public ScheduleWhenTuplesAvailable(TupleAvailabilityType type, int tupleCount, int... ports)
	{
		if (ports.length == 0)
		{
			throw new IllegalArgumentException();
		}

		for (int port : ports)
		{
			this.tupleCountByPortIndex.put(port, tupleCount);
		}

		this.tupleAvailabilityType = type;
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
		AVAILABLE_ON_ALL, AVAILABLE_ON_ANY
	}

}
