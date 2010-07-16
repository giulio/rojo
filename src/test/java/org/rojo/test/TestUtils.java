package org.rojo.test;

import java.util.ArrayList;
import java.util.List;

import org.rojo.repository.TypeConverter;
import org.rojo.repository.converters.Converters;
import org.rojo.repository.converters.IntegerConverter;
import org.rojo.repository.converters.StringConverter;

public class TestUtils {
    
    public static Converters initConverters() {
        List<TypeConverter> converters = new ArrayList<TypeConverter>(2);
        converters.add(new IntegerConverter());
        converters.add(new StringConverter());
        return new Converters(converters);
    }

}
