package cs.bilkent.zanza.operator;

import java.util.HashMap;
import java.util.Map;

public class OperatorConfig implements Fields
{

	private final Map<String, Object> values = new HashMap<>();

	private PartitionKeyExtractor partitionKeyExtractor;

	public OperatorConfig()
	{
	}

	public OperatorConfig(PartitionKeyExtractor partitionKeyExtractor)
	{
		this.partitionKeyExtractor = partitionKeyExtractor;
	}

	public OperatorConfig(Map<String, Object> values, PartitionKeyExtractor partitionKeyExtractor)
	{
		this.values.putAll(values);
		this.partitionKeyExtractor = partitionKeyExtractor;
	}

	public PartitionKeyExtractor getPartitionKeyExtractor()
	{
		return partitionKeyExtractor;
	}

	public void setPartitionKeyExtractor(PartitionKeyExtractor partitionKeyExtractor)
	{
		this.partitionKeyExtractor = partitionKeyExtractor;
	}

	public Map<String, Object> getValues()
	{
		return values;
	}

	@Override public <T> T get(String field)
	{
		return (T) values.get(field);
	}

	@Override public <T> T getOrDefault(String field, T defaultVal)
	{
		return (T) values.getOrDefault(field, defaultVal);
	}

	@Override public void set(String field, Object value)
	{
		this.values.put(field, value);
	}

	@Override public <T> T put(String field, T value)
	{
		return (T) this.values.put(field, value);
	}

	@Override public Object remove(String field)
	{
		return this.values.remove(field);
	}

	@Override public boolean delete(String field)
	{
		return this.values.remove(field) != null;
	}

	@Override public void clear()
	{
		this.values.clear();
	}

	@Override public int size()
	{
		return this.values.size();
	}

}
