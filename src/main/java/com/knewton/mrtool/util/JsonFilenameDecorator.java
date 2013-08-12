/**
 * Copyright 2013 Knewton
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
