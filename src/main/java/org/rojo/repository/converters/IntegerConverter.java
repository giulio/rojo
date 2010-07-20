package org.rojo.repository.converters;

import org.rojo.repository.TypeConverter;

public class IntegerConverter implements TypeConverter {

    @Override
    public boolean applyesFor(Class<? extends Object> type) {
        return type == Integer.class || type == int.class;
    }

    @Override
    public byte[] encode(Object object) {
        return object.toString().getBytes();
    }

    @Override
    public Object decode(byte[] bytes) {
        return Integer.parseInt(new String(bytes));
    }
    
}
