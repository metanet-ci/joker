package cs.bilkent.zanza.operator;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

// TODO Comparable, Serializable, Iterable
// supports immutable or effectively final objects
public class Tuple implements Fields
{

	private final Map<String, Object> values;

	public Tuple()
	{
		this.values = new HashMap<>();
	}

	public Tuple(boolean withInsertionOrder)
	{
		this.values = withInsertionOrder ? new LinkedHashMap<>() : new HashMap<>();
	}

	public Tuple(String field, Object value)
	{
		this.values = new HashMap<>();
		this.values.put(field, value);
	}

	public Tuple(Map<String, Object> values)
	{
		this.values = new HashMap<>();
		this.values.putAll(values);
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
