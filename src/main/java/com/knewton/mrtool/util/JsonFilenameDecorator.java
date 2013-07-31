/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.util;

/**
 * Decorator for decorating json strings with a field indicating the filename from which they were
 * extracted from.
 * 
 */
public class JsonFilenameDecorator implements ObjectDecorator<String> {

    private String decoratingStr;
    public static final String FILENAME_TAG = "filename";

    public JsonFilenameDecorator(String decoratingStr) {
        if (decoratingStr == null) {
            throw new IllegalArgumentException("Null decorating string.");
        }
        this.decoratingStr = decoratingStr;
    }

    /**
     * Adds the filename to the beginning of the json string. If the string is already decorated
     * then a new instance of the string is returned.
     * 
     * This method does not modify <code>str</code>
     * 
     * @param str
     *            The string to be decorated with the filename
     */
    @Override
    public String decorateObject(String str) {
        if (str.contains(FILENAME_TAG)) {
            return new String(str);
        } else {
            return str.replaceFirst("\\{", "{\"" + FILENAME_TAG + "\":\"" +
                    decoratingStr + "\",");
        }
    }

    public String getDecoratingString() {
        return decoratingStr;
    }

}
