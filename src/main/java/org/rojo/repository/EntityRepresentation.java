package org.rojo.repository;

import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;

/**
 * Encapsulate an entity. Provide accessors for @Id and @Value.
 *
 * Entities representation are cached since instantiation requires access to
 * Java reflection methods (time consuming)
 */
public class EntityRepresentation {

	private static final Map<Class<? extends Object>, EntityRepresentation> knownEntities;
	private final boolean cache;
	private Field id;
	private final Field[] fields;
	private Field unique;
	private final Map<String, Field> fieldMap = new HashMap<String, Field>();
	private String idGenerator;

	static {
		knownEntities = new HashMap<Class<? extends Object>, EntityRepresentation>();
	}

	public static EntityRepresentation forClass(Class<? extends Object> entityClass) {
		if (knownEntities.containsKey(entityClass)) {
			return knownEntities.get(entityClass);
		}
		EntityRepresentation entityRepresentation = new EntityRepresentation(entityClass);
		knownEntities.put(entityClass, entityRepresentation);
		return entityRepresentation;
	}

	/**
	 *
	 *
	 * @param entityClass
	 */
	private EntityRepresentation(Class<? extends Object> entityClass) {
		verifyEntityAnnotation(entityClass);
		cache = entityClass.getAnnotation(Entity.class).Cache();
		allField(entityClass);
		if (id == null) {
			error(entityClass, "missing @Id field!");
		}
		fields = new Field[fieldMap.size()];
		fieldMap.values().toArray(fields);
	}

	/**
	 * fields include super classes
	 *
	 * @param entityClass
	 * @return
	 */
	private void allField(Class<? extends Object> claz) {
		Field[] fs = claz.getDeclaredFields();
		for (Field f : fs) {
			if (f.isAnnotationPresent(Id.class)) {
				if (id == null) {
					if (!(f.getType() == long.class || f.getType() == Long.class)) {
						error(claz, "invalid @Id field type! accepted types are {long, Long}");
					}
					f.setAccessible(true);
					id = f;
					Id annotation = id.getAnnotation(Id.class);
					idGenerator = annotation.Generator();
				} else {
					error(claz, "duplicate @Id field type !");
				}
			} else if (f.isAnnotationPresent(Value.class)) {
				if (Collection.class.isAssignableFrom(f.getType())) {
					if (!(f.getType() == Set.class || f.getType() == List.class || f.getType() == Collection.class)) {
						error(claz, "only Collection, Set and List are supported");
					}
				}
				Value value = f.getAnnotation(Value.class);
				f.setAccessible(true);
				fieldMap.put(f.getName(), f);
				if (value.unique()) {
					if (unique == null) {
						this.unique = f;
					} else {
						error(claz, "more than one unique field !");
					}
				}
			}
		}
		Class sclaz = claz.getSuperclass();
		if (sclaz != null) {
			allField(sclaz);
		}
	}

	private void verifyEntityAnnotation(Class<? extends Object> entityClass) {
		if (entityClass.getAnnotation(Entity.class) == null) {
			error(entityClass, "missing @Entity annotation");
		}
	}

	private void error(Class<? extends Object> entityClass, String msg) {
		throw new InvalidTypeException(entityClass.getCanonicalName() + ": " + msg);
	}

	public Field getUnique() {
		return unique;
	}

	public String getIdGenerator() {
		return idGenerator;
	}

	public long getId(Object entity) {
		return readId(entity, id);
	}

	public void setId(Object entity, long idValue) {
		try {
			id.set(entity, idValue);
		} catch (Exception e) {
			throw new InvalidTypeException(e);
		}
	}

	public Field[] getFields() {
		return fields;
	}

	public Map<String, Field> getFieldMap() {
		return fieldMap;
	}

	public Field getField(String k) {
		return fieldMap.get(k);
	}

	private long readId(Object entity, Field id) {
		long returnValue = 0;
		try {
			returnValue = id.getLong(entity);
		} catch (IllegalAccessException e) {
			throw new InvalidTypeException(e);
		}
		return returnValue;
	}

	public boolean isCache() {
		return cache;
	}

}
