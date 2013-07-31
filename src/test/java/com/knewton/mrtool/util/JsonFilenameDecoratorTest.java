/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
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
