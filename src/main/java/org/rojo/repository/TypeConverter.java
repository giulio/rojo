package org.rojo.repository;

public interface TypeConverter {

        boolean applyesFor(Class<? extends Object> type);
    
        byte[] encode(Object object);
        
        Object decode(byte[] bytes);
    
}
