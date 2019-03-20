package com.xcar.hbase.core;

import com.jcraft.jsch.Logger;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: zhou.pengbo
 * \* Date: 2018/8/21
 * \* Time: 12:53
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */

@Slf4j
public class EasyProperties {

    private static EasyProperties instance = null;
    private String propertiesFileName ;

    private EasyProperties(String propertiesFileName){
        this.propertiesFileName=propertiesFileName;
    }

    public static EasyProperties getInstance(String propertiesFileName) {
        if (instance == null) {
            return new EasyProperties(propertiesFileName);
        }
        return instance;
    }

    public String getString(String key){
        String property = "";
        if (key == null || key.equals("") || key.equals("null")) {
            return "";
        }
        Properties prop = new Properties();
        try {
            prop.load(EasyProperties.class.getClassLoader().getResourceAsStream(propertiesFileName));
            property = prop.getProperty(key);
        } catch(IOException e) {
            log.info(e.getMessage(),e);
        }
        return property;
    }

    public Long getLong(String key) {
        String value = getString(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException nfe) {
                log.info(nfe.getMessage(),nfe);
                return null;
            }
        }
        return null;
    }

    public Integer getInteger(String key) {
        String value = getString(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException nfe) {
                log.info(nfe.getMessage(),nfe);
                return null;
            }
        }
        return null;
    }

    public Short getShort(String key) {
        String value = getString(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Short.parseShort(value);
            } catch (NumberFormatException nfe) {
                log.info(nfe.getMessage(),nfe);
                return null;
            }
        }
        return null;
    }

    public Double getDouble(String key) {
        String value = getString(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException nfe) {
                log.info(nfe.getMessage(),nfe);
                return null;
            }
        }
        return null;
    }

    public Boolean getBoolean(String key) {
        String value = getString(key);
        if (StringUtils.isNotBlank(value)) {
            try {
                return Boolean.parseBoolean(value);
            } catch (NumberFormatException nfe) {
                log.info(nfe.getMessage(),nfe);
                return null;
            }
        }
        return null;
    }

}