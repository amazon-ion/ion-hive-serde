package software.amazon.ionhiveserde.configuration

import software.amazon.ionhiveserde.configuration.source.RawConfiguration
import java.util.*

class MapBasedRawConfiguration(private val map: Map<String, String>) : RawConfiguration {
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