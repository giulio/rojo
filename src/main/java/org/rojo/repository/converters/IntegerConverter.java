package org.rojo.repository.converters;

import org.jredis.ri.alphazero.support.DefaultCodec;

import org.rojo.repository.TypeConverter;

public class IntegerConverter implements TypeConverter {

    @SuppressWarnings("rawtypes") 
    @Override
    public boolean applyesFor(Class type) {
        return type == Integer.class || type == int.class;
    }

    @Override
    public byte[] encode(Object object) {
        return DefaultCodec.<Integer>encode((Integer)object);     
    }

    @Override
    public Object decode(byte[] bytes) {
        return DefaultCodec.<Integer>decode(bytes);
    }
    
}
