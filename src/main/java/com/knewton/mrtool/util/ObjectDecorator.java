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
