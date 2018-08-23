/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>
 * An in memory <a href="https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)">Least recently used (LRU)</a> Cache
 * implementation
 * </p>
 *
 * @param <K> Key type
 * @param <V> Value Type
 */
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    // from java.util.HashMap
    private static final float DEFAULT_LOAD_FACTOR = 0.75f;
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private final int maxEntries;

    /**
     * Constructs an empty <tt>LRUCache</tt> instance with the
     * specified initial capacity and max entries.
     *
     * @param initialCapacity initial cache capacity, will grow to maxEntries
     * @param maxEntries      maximum number of entries
     */
    public LRUCache(int initialCapacity, int maxEntries) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR, maxEntries);
    }

    /**
     * Constructs an empty <tt>LRUCache</tt> instance with the
     * specified initial capacity, load factor and max entries.
     *
     * @param initialCapacity initial cache capacity, will grow to maxEntries
     * @param loadFactor      backing hash table load factor
     * @param maxEntries      maximum number of entries
     */
    public LRUCache(int initialCapacity, float loadFactor, int maxEntries) {
        super(initialCapacity, loadFactor, true); // true to use accessOrder as the ordering mode
        this.maxEntries = maxEntries;
    }

    /**
     * Constructs an empty <tt>LRUCache</tt> instance with the
     * specified max entries.
     *
     * @param maxEntries maximum number of entries
     */
    public LRUCache(int maxEntries) {
        this(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR, maxEntries);
    }

    /**
     * When this method returns true the backing {@link LinkedHashMap} removes the eldest entry. Since its ordering mode
     * is accessOrder the least recently will be the eldest entry
     */
    @Override
    protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
        return size() > maxEntries;
    }
}