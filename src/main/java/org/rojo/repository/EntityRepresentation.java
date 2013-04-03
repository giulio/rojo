package org.rojo.repository;

import org.rojo.annotations.Reference;
import org.rojo.annotations.Value;
import org.rojo.exceptions.InvalidTypeException;
import org.rojo.repository.utils.IdUtil;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulate an entity.
 * Provide accessors for @Id and @Attributes.
 *
 * Entities representation are cached since instantiation requires
 * access to Java reflection methods (time consuming)
 */
public class EntityRepresentation {

    private static EntityValidator validator  = new AnnotationValidator();

    private static Map<Class<? extends Object>, EntityRepresentation> knownEntities;
    static {
        knownEntities = new HashMap<Class<? extends Object>, EntityRepresentation>();
    }

    private Field id;
    private List<Field> values = new ArrayList<Field>();
    private List<Field> references = new ArrayList<Field>();

    public static EntityRepresentation forClass(Class<? extends Object> entityClass) {
        if (knownEntities.containsKey(entityClass)) return knownEntities.get(entityClass);
        EntityRepresentation entityRepresentation = new EntityRepresentation(entityClass);
        knownEntities.put(entityClass, entityRepresentation);
        return entityRepresentation;
    }

    private EntityRepresentation(Class<? extends Object> entityClass) {

        validator.validateEntity(entityClass);

        id = IdUtil.getIdField(entityClass);

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Value.class)) {
                values.add(field);
            }
            if (field.isAnnotationPresent(Reference.class)) {
                references.add(field);
            }
        }
    }

    public long getId(Object entity) {
        return IdUtil.readId(entity, id);
    }

    public void setId(Object entity, long idValue) {
        boolean idFieldAccessibility = id.isAccessible();
        if (!idFieldAccessibility) id.setAccessible(true);
        try {
            id.set(entity, idValue);
        } catch (Exception e) {
            new InvalidTypeException(e);
        } finally {
            if (!idFieldAccessibility) id.setAccessible(idFieldAccessibility);
        }
    }

    public List<Field> getValues() {
        return values;
    }

    public List<Field> getReferences() {
        return references;
    }
}
