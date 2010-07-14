package org.rojo.repository;

public interface TypeConverter {

        boolean applyesFor(@SuppressWarnings("rawtypes") Class type);
    
        byte[] encode(Object object);
        
        Object decode(byte[] bytes);
    
}
