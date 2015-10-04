package cs.bilkent.zanza.operator;

import java.util.Collection;
import java.util.Collections;
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

	public Tuple(final boolean withInsertionOrder)
	{
		this.values = withInsertionOrder ? new LinkedHashMap<>() : new HashMap<>();
	}

	public Tuple(final String field, final Object value)
	{
		this.values = new HashMap<>();
		this.values.put(field, value);
	}

	public Tuple(final Map<String, Object> values)
	{
		this.values = new HashMap<>();
		this.values.putAll(values);
	}

	public Map<String, Object> getValues()
	{
		return values;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T get(final String field)
	{
		return (T) values.get(field);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getOrDefault(final String field, final T defaultVal)
	{
		return (T) values.getOrDefault(field, defaultVal);
	}

	@Override
	public void set(final String field, final Object value)
	{
		this.values.put(field, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T put(final String field, final T value)
	{
		return (T) this.values.put(field, value);
	}

	@Override
	public Object remove(final String field)
	{
		return this.values.remove(field);
	}

	@Override
	public boolean delete(final String field)
	{
		return this.values.remove(field) != null;
	}

	@Override
	public void clear()
	{
		this.values.clear();
	}

	@Override
	public int size()
	{
		return this.values.size();
	}

	@Override
	public Collection<String> fieldNames()
	{
		return Collections.unmodifiableCollection(values.keySet());
	}
}
