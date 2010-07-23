package org.rojo.exceptions;

@SuppressWarnings("serial")
public class MissingEntity extends RepositoryError {

    private Class<? extends Object> entityClass;
    private long id;
    
    public MissingEntity(Class<? extends Object> entityClass,
            long id) {
        super("missing entity " + entityClass + ", id:" + id);
        this.entityClass = entityClass;
        this.id = id;
    }

    public Class<? extends Object> getEntityClass() {
        return entityClass;
    }

    public long getId() {
        return id;
    }
    
}
