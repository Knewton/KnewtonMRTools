/**
 * Copyright (c) 2013 Knewton
 * 
 * Dual licensed under: MIT: http://www.opensource.org/licenses/mit-license.php GPLv3:
 * http://www.opensource.org/licenses/gpl-3.0.html.
 * 
 */
package com.knewton.mrtool.util;

/**
 * Performs a decorating action on an object. For example this is used by the file consolidator to
 * decorate an object with the original filename
 * 
 * @author Giannis Neokleous
 * 
 * @param <T>
 */
public interface ObjectDecorator<T> {

    /**
     * Decorate an object of type T.
     * 
     * @param object
     * @return
     */
    public T decorateObject(T object);

}
