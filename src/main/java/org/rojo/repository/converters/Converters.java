package org.rojo.repository.converters;


import java.util.List;

import org.rojo.exceptions.ConversionException;
import org.rojo.repository.TypeConverter;

public class Converters {

    List<TypeConverter> converters;
    
    public Converters(List<TypeConverter> converters) {
        this.converters = converters;
    }

    public TypeConverter getConverterFor(Class<? extends Object> type) {
        for (TypeConverter converter : converters) {
            if (converter.applyesFor(type)) {
                return converter;
            }
        }
        throw new ConversionException("cannot find a suitable converter for type: " + type);
    }



}
