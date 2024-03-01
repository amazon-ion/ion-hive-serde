package com.amazon.ionhiveserde.configuration

import com.amazon.ion.IonType
import junitparams.JUnitParamsRunner
import junitparams.Parameters
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.test.assertEquals

@RunWith(JUnitParamsRunner::class)
class SerializeAsConfigTest {

    private fun makeConfig(map: Map<String, String>, types: List<TypeInfo>) = SerializeAsConfig(
        MapBasedRawConfiguration(map), types
    )

    private fun defaultSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.BOOL),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.intTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.longTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.TIMESTAMP),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.TIMESTAMP),

            // default for configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.charTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.varcharTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.binaryTypeInfo, IonType.BLOB),
            listOf(TypeInfoFactory.decimalTypeInfo, IonType.DECIMAL)
    )

    @Test
    @Parameters(method = "defaultSerializationMappings")
    fun serializationIonTypeForDefaults(typeInfo: TypeInfo, expectedIonType: IonType) {
        val subject = makeConfig(mapOf(), listOf(typeInfo))

        assertEquals(expectedIonType, subject.serializationIonTypeFor(0))
    }

    private fun validSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.BOOL),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.intTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.longTypeInfo, IonType.INT),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.TIMESTAMP),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.TIMESTAMP),

            // configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.stringTypeInfo, IonType.SYMBOL),
            listOf(TypeInfoFactory.charTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.charTypeInfo, IonType.SYMBOL),
            listOf(TypeInfoFactory.varcharTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.varcharTypeInfo, IonType.SYMBOL),
            listOf(TypeInfoFactory.binaryTypeInfo, IonType.BLOB),
            listOf(TypeInfoFactory.binaryTypeInfo, IonType.CLOB),
            listOf(TypeInfoFactory.decimalTypeInfo, IonType.DECIMAL),
            listOf(TypeInfoFactory.decimalTypeInfo, IonType.INT)
    )

    @Test
    @Parameters(method = "validSerializationMappings")
    fun serializationIonType(typeInfo: TypeInfo, expectedIonType: IonType) {
        val subject = makeConfig(
                mapOf("ion.column[0].serialize_as" to expectedIonType.name),
                listOf(typeInfo))

        assertEquals(expectedIonType, subject.serializationIonTypeFor(0))
    }

    private fun invalidSerializationMappings() = listOf<Any>(
            // fixed
            listOf(TypeInfoFactory.booleanTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.byteTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.shortTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.intTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.longTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.floatTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.doubleTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.dateTypeInfo, IonType.STRING),
            listOf(TypeInfoFactory.timestampTypeInfo, IonType.STRING),

            // configurable
            listOf(TypeInfoFactory.stringTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.charTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.varcharTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.binaryTypeInfo, IonType.FLOAT),
            listOf(TypeInfoFactory.decimalTypeInfo, IonType.FLOAT)
    )

    @Test(expected = IllegalArgumentException::class)
    @Parameters(method = "invalidSerializationMappings")
    fun serializationIonTypeInvalid(typeInfo: TypeInfo, expectedIonType: IonType) {
        makeConfig(
                mapOf("ion.column[0].serialize_as" to expectedIonType.name),
                listOf(typeInfo)
        ).serializationIonTypeFor(0)
    }

    @Test(expected = IndexOutOfBoundsException::class)
    fun serializationIonTypeUnknownColumn() {
        makeConfig(
                mapOf("ion.column[0].serialize_as" to IonType.INT.name),
                listOf(TypeInfoFactory.intTypeInfo)
        ).serializationIonTypeFor(1)
    }
}
