package com.company;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by mialiu on 10/11/15.
 */
public class Config {
    private Properties _properties = null;
    private FileInputStream _stream = null;

    public Config () {
        _properties = new Properties();
    }

    public Config (String path) {
        _properties = new Properties();
        try {
            _stream = new FileInputStream(path);
            _properties.load(_stream);
            _stream.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public String getValue(String key) {
        return _properties.getProperty(key);
    }
}
