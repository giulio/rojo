package org.rojo.repository;

public interface TypeConverter {

        boolean applyesFor(Class<? extends Object> type);
    
        String encode(Object object);
        
        Object decode(String bytes);
    
}
