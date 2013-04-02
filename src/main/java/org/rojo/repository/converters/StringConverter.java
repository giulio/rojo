package org.rojo.repository.converters;

import org.rojo.repository.TypeConverter;

public class StringConverter implements TypeConverter {

    @Override
    public boolean applyesFor(Class<? extends Object> type) {
        return type == String.class;
    }

    @Override
    public String encode(Object object) {
        return ((String)object);
    }

    @Override
    public Object decode(String string) {
        return string;
    }
    
}
