package com.hzx.util;

import java.util.Properties;

public class PropertiesException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = 4372660914816539735L;
    private Properties mProperties;
    private String            mKey;

    /**
     * @param aProperties the property object searched for a property
     * @param aKey key of the missing property
     */
    public PropertiesException(Properties aProperties, String aKey) {
        super("Error while retrieving property '" + aKey + "' from configuration.");
        mProperties = aProperties;
        mKey = aKey;
    }

    /**
     * @param aMessage error message
     * @param aProperties the property object searched for a property
     * @param aKey key of the missing property
     */
    public PropertiesException(String aMessage, Properties aProperties, String aKey) {
        super(aMessage);
        mProperties = aProperties;
        mKey = aKey;
    }

    /**
     * @param aMessage error message
     * @param aThrowable cause of the exception
     * @param aProperties the property object searched for a property
     * @param aKey key of the missing property
     */
    public PropertiesException(String aMessage, Throwable aThrowable, Properties aProperties, String aKey) {
        super(aMessage, aThrowable);
        mProperties = aProperties;
        mKey = aKey;
    }

    /**
     * @return property object missing a property and therefore causing the exception
     */
    public Properties getProperties() {
        return mProperties;
    }

    /**
     * property key causing the exception
     *
     * @return
     */
    public String getKey() {
        return mKey;
    }
}
