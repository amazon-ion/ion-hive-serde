/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *      http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.objectinspectors

import org.apache.hadoop.hive.common.type.HiveVarchar
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable
import org.junit.Test
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class IonTextToVarcharObjectInspectorTest : AbstractIonTextToMaxLengthObjectInspectorTest() {

    protected override val subject = IonTextToVarcharObjectInspector(maxLength)

    @Test
    fun getPrimitiveWritableObjectWithString() {
        val string = ION.newString(valid)

        val actual = subject.getPrimitiveWritableObject(string)
        assertEquals(HiveVarcharWritable(HiveVarchar(valid, maxLength)), actual)
    }

    @Test
    fun getPrimitiveWritableObjectWithSymbol() {
        val symbol = ION.newSymbol(valid)

        val actual = subject.getPrimitiveWritableObject(symbol)
        assertEquals(HiveVarcharWritable(HiveVarchar(valid, maxLength)), actual)
    }

    @Test
    fun getPrimitiveJavaObjectWithString() {
        val string = ION.newString(valid)

        val actual = subject.getPrimitiveJavaObject(string)
        assertTrue(HiveVarchar(valid, maxLength).equals(actual))
    }

    @Test
    fun getPrimitiveJavaObjectWithSymbol() {
        val symbol = ION.newSymbol(valid)

        val actual = subject.getPrimitiveJavaObject(symbol)
        assertTrue(HiveVarchar(valid, maxLength).equals(actual))
    }
}