package org.rojo.repository.converters;

import org.rojo.repository.TypeConverter;

public class IntegerConverter implements TypeConverter {

    @Override
    public boolean applyesFor(Class<? extends Object> type) {
        return type == Integer.class || type == int.class;
    }

    @Override
    public String encode(Object object) {
        return object.toString();
    }

    @Override
    public Object decode(String string) {
        return Integer.parseInt(string);
    }
    
}
