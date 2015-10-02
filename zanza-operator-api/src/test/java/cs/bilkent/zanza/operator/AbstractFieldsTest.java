package cs.bilkent.zanza.operator;

import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class AbstractFieldsTest
{

	private Fields fields;

	@Before public final void createFieldsInstance()
	{
		fields = newFieldsInstance();
	}

	protected abstract Fields newFieldsInstance();

	protected <T extends Fields> T getFields()
	{
		return (T) fields;
	}

	@Test public void shouldSet()
	{
		fields.set("field", "value");
		assertThat(fields.getObject("field"), equalTo("value"));
	}

	@Test public void shouldPut()
	{
		fields.set("field", "value");
		assertThat(fields.put("field", "value2"), equalTo("value"));
	}

	@Test public void shouldRemove()
	{
		fields.set("field", "value");
		assertNotNull(fields.remove("field"));
		assertNull(fields.getObject("field"));
	}

	@Test public void shouldNotRemoveNonExistingField()
	{
		assertNull(fields.remove("field"));
	}

	@Test public void shouldDelete()
	{
		fields.set("field", "value");
		assertTrue(fields.delete("field"));
		assertNull(fields.getObject("field"));
	}

	@Test public void shouldNotDeleteNonExistingField()
	{
		assertFalse(fields.delete("field"));
	}

	@Test public void shouldGetExisting()
	{
		fields.set("field", "value");
		assertThat(fields.<String>get("field"), equalTo("value"));
	}

	@Test public void shouldGetExistingOrDefault()
	{
		fields.set("field", "value");
		assertThat(fields.getOrDefault("field", "value2"), equalTo("value"));
	}

	@Test public void shouldGetNonExistingOrDefault()
	{
		assertThat(fields.getOrDefault("field", "value"), equalTo("value"));
	}

	@Test public void shouldContainExistingField()
	{
		fields.put("field", "value");
		assertTrue(fields.contains("field"));
	}

	@Test public void shouldNotContainNonExistingField()
	{
		assertFalse(fields.contains("field"));
	}

	@Test public void shouldGetExistingObject()
	{
		fields.put("field", "value");
		assertThat(fields.getObject("field"), equalTo("value"));
	}

	@Test public void shouldNotGetNonExistingObject()
	{
		assertNull(fields.getObject("field"));
	}

	@Test public void shouldGetExistingObjectOrDefault()
	{
		fields.put("field", "value");
		assertThat(fields.getObjectOrDefault("field", "value2"), equalTo("value"));
	}

	@Test public void shouldGetNonExistingObjectOrDefault()
	{
		assertThat(fields.getObjectOrDefault("field", "value2"), equalTo("value2"));
	}

	@Test public void shouldGetExistingString()
	{
		fields.put("field", "value");
		assertThat(fields.getString("field"), equalTo("value"));
	}

	@Test public void shouldNotGetExistingString()
	{
		assertNull(fields.getString("field"));
	}

	@Test public void shouldGetExistingStringOrDefault()
	{
		fields.put("field", "value");
		assertThat(fields.getStringOrDefault("field", "value2"), equalTo("value"));
	}

	@Test public void shouldGetNonExistingStringOrDefault()
	{
		assertThat(fields.getStringOrDefault("field", "value2"), equalTo("value2"));
	}

	@Test public void shouldGetExistingInteger()
	{
		fields.put("field", 1);
		assertThat(fields.getInteger("field"), equalTo(1));
	}

	@Test public void shouldNotGetNotExistingInteger()
	{
		assertNull(fields.getInteger("field"));
	}

	@Test public void shouldGetExistingIntegerOrDefault()
	{
		fields.put("field", 1);
		assertThat(fields.getIntegerOrDefault("field", 2), equalTo(1));
	}

	@Test public void shouldGetNonExistingIntegerOrDefault()
	{
		assertThat(fields.getIntegerOrDefault("field", 2), equalTo(2));
	}

	@Test public void shouldGetExistingLong()
	{
		fields.put("field", 1L);
		assertThat(fields.getLong("field"), equalTo(1L));
	}

	@Test public void shouldNotGetExistingLong()
	{
		assertNull(fields.getLong("field"));
	}

	@Test public void shouldGetExistingLongOrDefault()
	{
		fields.put("field", 1L);
		assertThat(fields.getLongOrDefault("field", 2L), equalTo(1L));
	}

	@Test public void shouldGetNonExistingLongOrDefault()
	{
		assertThat(fields.getLongOrDefault("field", 2L), equalTo(2L));
	}

	@Test public void shouldGetExistingBoolean()
	{
		fields.put("field", Boolean.TRUE);
		assertTrue(fields.getBoolean("field"));
	}

	@Test public void shouldNotGetNonExistingBoolean()
	{
		assertNull(fields.getBoolean("field"));
	}

	@Test public void shouldGetExistingBooleanOrDefault()
	{
		fields.put("field", Boolean.TRUE);
		assertTrue(fields.getBooleanOrDefault("field", Boolean.FALSE));
	}

	@Test public void shouldGetNonExistingBooleanOrDefault()
	{
		assertTrue(fields.getBooleanOrDefault("field", Boolean.TRUE));
	}

	@Test public void shouldGetGetExistingShort()
	{
		short val = 1;
		fields.put("field", val);
		assertThat(fields.getShort("field"), equalTo(val));
	}

	@Test public void shouldNotGetNonExistingShort()
	{
		assertNull(fields.getShort("field"));
	}

	@Test public void shouldGetExistingShortOrDefault()
	{
		short val = 1;
		short other = 2;
		fields.put("field", val);
		assertThat(fields.getShortOrDefault("field", other), equalTo(val));
	}

	@Test public void shouldGetNonExistingShortOrDefault()
	{
		short val = 1;
		assertThat(fields.getShortOrDefault("field", val), equalTo(val));
	}

	@Test public void shouldGetExistingByte()
	{
		byte val = 1;
		fields.put("field", val);
		assertThat(fields.getByte("field"), equalTo(val));
	}

	@Test public void shouldNotGetNonExistingByte()
	{
		assertNull(fields.getByte("field"));
	}

	@Test public void shouldGetExistingByteOrDefault()
	{
		byte val = 1;
		byte other = 1;
		fields.put("field", val);
		assertThat(fields.getByteOrDefault("field", other), equalTo(val));
	}

	@Test public void shouldGetNonExistingByteOrDefault()
	{
		byte val = 1;
		assertThat(fields.getByteOrDefault("field", val), equalTo(val));
	}

	@Test public void shouldGetExistingDouble()
	{
		fields.put("field", 1d);
		assertThat(fields.getDouble("field"), equalTo(1d));
	}

	@Test public void shouldNotGetNonExistingDouble()
	{
		assertNull(fields.getDouble("field"));
	}

	@Test public void shouldGetExistingDoubleOrDefault()
	{
		fields.put("field", 1d);
		assertThat(fields.getDoubleOrDefault("field", 2d), equalTo(1d));
	}

	@Test public void shouldGetNonExistingDoubleOrDefault()
	{
		assertThat(fields.getDoubleOrDefault("field", 1d), equalTo(1d));
	}

	@Test public void shouldGetExistingFloat()
	{
		fields.put("field", 1f);
		assertThat(fields.getFloat("field"), equalTo(1f));
	}

	@Test public void shouldNotGetNonExistingFloat()
	{
		assertNull(fields.getFloat("field"));
	}

	@Test public void shouldGetExistingFloatOrDefault()
	{
		fields.put("field", 1f);
		assertThat(fields.getFloatOrDefault("field", 2f), equalTo(1f));
	}

	@Test public void shouldGetNonExistingFloatOrDefault()
	{
		assertThat(fields.getFloatOrDefault("field", 1f), equalTo(1f));
	}

	@Test public void shouldGetExistingBinary()
	{
		byte[] val = new byte[] { 1 };
		fields.put("field", val);
		assertThat(fields.getBinary("field"), equalTo(val));
	}

	@Test public void shouldGetExistingBinaryOrDefault()
	{
		byte[] val = new byte[] { 1 };
		fields.put("field", val);
		assertThat(fields.getBinaryOrDefault("field", new byte[] {}), equalTo(val));
	}

	@Test public void shouldGetNonExistingBinaryOrDefault()
	{
		byte[] val = new byte[] { 1 };
		assertThat(fields.getBinaryOrDefault("field", val), equalTo(val));
	}

	@Test public void shouldGetExistingCollection()
	{
		Collection<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getCollection("field"), equalTo(value));
	}

	@Test public void shouldNotGetNonExistingCollection()
	{
		assertNull(fields.getCollection("field"));
	}

	@Test public void shouldGetExistingCollectionOrDefault()
	{
		Collection<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getCollectionOrDefault("field", Collections.emptyList()), equalTo(value));
	}

	@Test public void shouldGetNonExistingCollectionOrDefault()
	{
		Collection<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		assertThat(fields.getCollectionOrDefault("field", value), equalTo(value));
	}

	@Test public void shouldGetExistingCollectionOrEmpty()
	{
		Collection<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getCollectionOrEmpty("field"), equalTo(value));
	}

	@Test public void shouldGetNonExistingCollectionOrEmpty()
	{
		assertThat(fields.getCollectionOrEmpty("field"), empty());
	}

	@Test public void shouldGetExistingList()
	{
		List<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getList("field"), equalTo(value));
	}

	@Test public void shouldNotGetNonExistingList()
	{
		assertNull(fields.getList("field"));
	}

	@Test public void shouldGetExistingListOrDefault()
	{
		List<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getListOrDefault("field", Collections.emptyList()), equalTo(value));
	}

	@Test public void shouldGetNonExistingListOrDefault()
	{
		List<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		assertThat(fields.getListOrDefault("field", value), equalTo(value));
	}

	@Test public void shouldGetExistingListOrEmpty()
	{
		List<Integer> value = new ArrayList<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getListOrEmpty("field"), equalTo(value));
	}

	@Test public void shouldGetNonExistingListOrEmpty()
	{
		assertThat(fields.getListOrEmpty("field"), empty());
	}

	@Test public void shouldGetExistingSet()
	{
		Set<Integer> value = new HashSet<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getSet("field"), equalTo(value));
	}

	@Test public void shouldNotGetNonExistingSet()
	{
		assertNull(fields.getSet("field"));
	}

	@Test public void shouldGetExistingSetOrDefault()
	{
		Set<Integer> value = new HashSet<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getSetOrDefault("field", Collections.emptySet()), equalTo(value));
	}

	@Test public void shouldGetNonExistingSetOrDefault()
	{
		Set<Integer> value = new HashSet<Integer>()
		{{
				add(1);
			}};
		assertThat(fields.getSetOrDefault("field", value), equalTo(value));
	}

	@Test public void shouldGetExistingSetOrEmpty()
	{
		Set<Integer> value = new HashSet<Integer>()
		{{
				add(1);
			}};
		fields.put("field", value);
		assertThat(fields.getSetOrEmpty("field"), equalTo(value));
	}

	@Test public void shouldGetNonExistingSetOrEmpty()
	{
		assertThat(fields.getSetOrEmpty("field"), empty());
	}

	@Test public void shouldGetExistingMap()
	{
		Map<Integer, Integer> value = new HashMap<Integer, Integer>()
		{{
				put(1, 1);
			}};
		fields.put("field", value);
		assertThat(fields.getMap("field"), equalTo(value));
	}

	@Test public void shouldNotGetNonExistingMap()
	{
		assertNull(fields.getMap("field"));
	}

	@Test public void shouldGetExistingMapOrDefault()
	{
		Map<Integer, Integer> value = new HashMap<Integer, Integer>()
		{{
				put(1, 1);
			}};
		fields.put("field", value);
		assertThat(fields.getMapOrDefault("field", Collections.emptyMap()), equalTo(value));
	}

	@Test public void shouldGetNonExistingMapOrDefault()
	{
		Map<Integer, Integer> value = new HashMap<Integer, Integer>()
		{{
				put(1, 1);
			}};
		assertThat(fields.getMapOrDefault("field", value), equalTo(value));
	}

	@Test public void shouldGetExistingMapOrEmpty()
	{
		Map<Integer, Integer> value = new HashMap<Integer, Integer>()
		{{
				put(1, 1);
			}};
		fields.put("field", value);
		assertThat(fields.getMapOrEmpty("field"), equalTo(value));
	}

	@Test public void shouldGetNonExistingMapOrEmpty()
	{
		assertThat(fields.getMapOrEmpty("field").size(), equalTo(0));
	}
}
