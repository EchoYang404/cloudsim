package org.bjut.hdfssim;

import org.apache.commons.configuration2.*;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.File;

public class Configuration {
    private static org.apache.commons.configuration2.Configuration config = null;
    private static String base = Configuration.class.getResource("/").getFile();;
    static {
        try {
            File file = new File(base + "config.properties");
            config = new Configurations().properties(file);
        } catch (ConfigurationException e) {
            e.printStackTrace();
        }
    }

    public static String getBasePath()
    {
        return base;
    }


    public static String getStringProperty(String name)
    {
        try {
            return config.getString(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Integer getIntProperty(String name)
    {
        try {
            return config.getInt(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static Double getDoubleProperty(String name)
    {
        try {
            return config.getDouble(name);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
