package org.rojo.repository;

import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.rojo.annotations.Entity;
import org.rojo.annotations.Id;
import org.rojo.annotations.Index;
import org.rojo.exceptions.RojoException;

/**
 * Encapsulate an entity. Provide accessors for @Id and @Value.
 *
 * Entities representation are cached since instantiation requires access to
 * Java reflection methods (time consuming)
 */
public class EntityRepresentation
{

  private static final Map<Class<? extends Object>, EntityRepresentation> knownEntities;
  private static final Map<String, EntityRepresentation> tableEntities;
  private final boolean cache;
  private String table;
  private Field id;
  private final Field[] fields;
  private final String[] columns;
  private Field unique;
  private final Map<String, Field> fieldMap = new HashMap<String, Field>();
  private final Map<String, String> columnMap = new HashMap<String, String>();
  private final List<Field> indexes = new ArrayList<Field>();
  private IdGenerator idGenerator;

  static
  {
    knownEntities = new HashMap<Class<? extends Object>, EntityRepresentation>();
    tableEntities = new HashMap<String, EntityRepresentation>();
  }

  public static EntityRepresentation forClass(Class<? extends Object> entityClass)
  {
    if (knownEntities.containsKey(entityClass))
    {
      return knownEntities.get(entityClass);
    }
    EntityRepresentation entityRepresentation = new EntityRepresentation(entityClass);
    knownEntities.put(entityClass, entityRepresentation);
    EntityRepresentation old = tableEntities.put(entityRepresentation.table, entityRepresentation);
    if (old != null)
    {
      throw new RojoException("duplicate table:" + entityRepresentation.table);
    }
    return entityRepresentation;
  }

  /**
   *
   *
   * @param entityClass
   */
  private EntityRepresentation(Class<? extends Object> entityClass)
  {
    verifyEntityAnnotation(entityClass);
    cache = entityClass.getAnnotation(Entity.class).cache();
    table = entityClass.getAnnotation(Entity.class).table();
    if (table.isEmpty())
    {
      table = entityClass.getSimpleName();
    }
    allField(entityClass);
    if (id == null)
    {
      error(entityClass, "missing @Id field!");
    }
    fields = new Field[fieldMap.size()];
    columns = new String[fieldMap.size()];
    fieldMap.values().toArray(fields);
    for (int i = 0; i < columns.length; i++)
    {
      columns[i] = fields[i].getAnnotation(Value.class).column();
      if (columns[i].isEmpty())
      {
        columns[i] = fields[i].getName();
      }
      columnMap.put(fields[i].getName(), columns[i]);
    }
  }

  /**
   * fields include super classes
   *
   * @param entityClass
   * @return
   */
  private void allField(Class<? extends Object> claz)
  {
    Field[] fs = claz.getDeclaredFields();
    for (Field f : fs)
    {
      if (f.isAnnotationPresent(Id.class))
      {
        if (id == null)
        {
          if (!(f.getType() == String.class))
          {
            error(claz, "invalid @Id field type! accepted types are {String}");
          }
          f.setAccessible(true);
          id = f;
          Id annotation = id.getAnnotation(Id.class);
          idGenerator = IdGenerator.getGenerator(annotation.generator());
          if (idGenerator == null)
          {
            error(claz, "idGenerator is null!");
          }
        } else
        {
          error(claz, "duplicate @Id field type !");
        }
      } else if (f.isAnnotationPresent(Value.class))
      {
        if (Collection.class.isAssignableFrom(f.getType()))
        {
          if (!(f.getType() == Set.class || f.getType() == List.class || f.getType() == Collection.class))
          {
            error(claz, "only Collection, Set and List are supported");
          }
        }
        Value value = f.getAnnotation(Value.class);
        f.setAccessible(true);
        fieldMap.put(f.getName(), f);
        if (f.isAnnotationPresent(Index.class))
        {
          this.indexes.add(f);
        }
        if (value.unique())
        {
          if (unique == null)
          {
            this.unique = f;
          } else
          {
            error(claz, "more than one unique field !");
          }
        }
      }
    }
    Class sclaz = claz.getSuperclass();
    if (sclaz != null)
    {
      allField(sclaz);
    }
  }

  private void verifyEntityAnnotation(Class<? extends Object> entityClass)
  {
    if (entityClass.getAnnotation(Entity.class) == null)
    {
      error(entityClass, "missing @Entity annotation");
    }
  }

  private void error(Class<? extends Object> entityClass, String msg)
  {
    throw new InvalidTypeException(entityClass.getCanonicalName() + ": " + msg);
  }

  public Field getUnique()
  {
    return unique;
  }

  public IdGenerator getIdGenerator()
  {
    return idGenerator;
  }

  public String getTable()
  {
    return table;
  }

  public String[] getColumns()
  {
    return columns;
  }

  public String getId(Object entity)
  {
    return readId(entity, id);
  }

  public void setId(Object entity, String idValue)
  {
    try
    {
      id.set(entity, idValue);
    } catch (Exception e)
    {
      throw new InvalidTypeException(e);
    }
  }

  public Field[] getFields()
  {
    return fields;
  }

  public Map<String, Field> getFieldMap()
  {
    return fieldMap;
  }

  public Field getField(String k)
  {
    return fieldMap.get(k);
  }

  String getColumn(String f)
  {
    return columnMap.get(f);
  }

  private String readId(Object entity, Field id)
  {
    String returnValue = null;
    try
    {
      Object o = id.get(entity);
      returnValue = o == null ? null : o.toString();
    } catch (Exception e)
    {
      throw new RojoException("access error" + e.getMessage());
    }
    return returnValue;
  }

  public boolean isCache()
  {
    return cache;
  }

  Object readProperty(Object entity, Field f)
  {
    try
    {
      Object o = f.get(entity);
      return o;
    } catch (Exception ex)
    {
      throw new RojoException("access error" + ex.getMessage());
    }
  }

  /**
   * indexed fields
   *
   * @return
   */
  public List<Field> getIndexes()
  {
    return indexes;
  }

  /**
   * sample entity
   *
   * @return
   */
  boolean isSampleEntity()
  {
    return this.unique == null && this.indexes.isEmpty();
  }
}
