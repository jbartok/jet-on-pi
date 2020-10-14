package com.hazelcast.jet.pi.impl;

import com.hazelcast.jet.pi.Latency;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Util {

    public static Properties loadProps(Class clazz, String propsFilename) {
        InputStream inputStream = clazz.getClassLoader().getResourceAsStream(propsFilename);

        if (inputStream == null) {
            System.err.println("Properties file not found");
            System.exit(1);
        } else {
            try {
                Properties props = new Properties();
                props.load(inputStream);
                return props;
            } catch (IOException e) {
                System.err.println("Can't read properties file");
                System.exit(2);
            }
        }

        return null;
    }

    public  static String ensureProp(Properties props, String propName) throws ValidationException {
        String prop = props.getProperty(propName);
        if (prop == null || prop.isEmpty()) {
            throw new ValidationException("Missing property: " + propName);
        }
        return prop;
    }

    public static int parseIntProp(Properties props, String propName) throws ValidationException {
        String prop = ensureProp(props, propName);
        try {
            int value = Integer.parseInt(prop.replace("_", ""));
            System.out.println(propName + " = " + value);
            return value;
        } catch (NumberFormatException e) {
            throw new ValidationException("Invalid property format, correct example is 9_999: " + propName + "=" + prop);
        }
    }

}
