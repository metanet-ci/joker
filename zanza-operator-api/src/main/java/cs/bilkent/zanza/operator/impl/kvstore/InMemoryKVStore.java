package cs.bilkent.zanza.operator.impl.kvstore;

import cs.bilkent.zanza.operator.kvstore.KVStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryKVStore implements KVStore
{

	private final Map<String, Object> values = new ConcurrentHashMap<>();

	public InMemoryKVStore()
	{
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
		values.put(field, value);
	}

	@Override public <T> T put(String field, T value)
	{
		return (T) values.put(field, value);
	}

	@Override public Object remove(String field)
	{
		return values.remove(field);
	}

	@Override public boolean delete(String field)
	{
		return values.remove(field) != null;
	}
}
