package com.amazon.ionhiveserde.configuration

import java.util.*

class MapBasedRawConfiguration(private val map: Map<String, String>) : com.amazon.ionhiveserde.configuration.source.RawConfiguration {
    override fun get(key: String?): Optional<String> {
        return Optional.ofNullable(map[key])
    }

    override fun containsKey(propertyKey: String?): Boolean {
        return map.containsKey(propertyKey)
    }

    override fun getOrDefault(key: String?, defaultValue: String?): String {
        return map[key] ?: defaultValue!!
    }
}