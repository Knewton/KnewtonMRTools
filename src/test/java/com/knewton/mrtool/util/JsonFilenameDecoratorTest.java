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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests the {@link JsonFilenameDecorator}
 * 
 * @author Giannis Neokleous
 * 
 */
public class JsonFilenameDecoratorTest {

    /**
     * Tests the json filename decorator to see if a string is properly decorated with a filename
     * tag.
     */
    @Test
    public void testJsonFilenameDecorator() {
        String jsonStr = "{}";
        JsonFilenameDecorator jsonDecorator =
                new JsonFilenameDecorator("filename.txt");
        assertEquals("filename.txt", jsonDecorator.getDecoratingString());
        String result = jsonDecorator.decorateObject(jsonStr);
        assertEquals("{}", jsonStr);
        assertEquals("{\"" + JsonFilenameDecorator.FILENAME_TAG +
                "\":\"filename.txt\",}", result);

        jsonStr = "{\"anothertag\":4}";
        result = jsonDecorator.decorateObject(jsonStr);
        assertEquals("{\"anothertag\":4}", jsonStr);
        assertEquals("{\"" + JsonFilenameDecorator.FILENAME_TAG +
                "\":\"filename.txt\",\"anothertag\":4}", result);
    }

    /**
     * Tests to see if a null pointer exception is thrown by the decorator when the string being
     * decorated is null.
     */
    @Test(expected = NullPointerException.class)
    public void testExceptionFilenameDecorator() {
        JsonFilenameDecorator jsonDecorator =
                new JsonFilenameDecorator("randomStr");
        jsonDecorator.decorateObject(null);
    }

    /**
     * Decorator throws an exception when passed a null decorating string.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExceptionInConstructorForFilenameDecorator() {
        new JsonFilenameDecorator(null);
    }

    /**
     * Tests to see if an already decorated string with a filename tag is not decorated again.
     */
    @Test
    public void testDoubleDecoratedString() {
        // An already decorated string.
        String jsonStr = "{\"" + JsonFilenameDecorator.FILENAME_TAG +
                "\":\"filename.txt\",}";

        JsonFilenameDecorator jsonDecorator =
                new JsonFilenameDecorator("filename2.txt");
        String decStr = jsonDecorator.decorateObject(jsonStr);
        assertEquals(jsonStr, decStr);

    }

}
