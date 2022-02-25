/*
 * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.*
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.io.Text

class IonValueToStringObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest<IonValue, Text, String>() {

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonValueToStringObjectInspector()
    override fun validTestCases() = listOf(
            ValidTestCase(generateTestIonValues(IonType.STRING), "some string", Text("some string")),
            ValidTestCase(generateTestIonValues(IonType.SYMBOL), "some string", Text("some string")),
            ValidTestCase(generateTestIonValues(IonType.DECIMAL), "12.1", Text("12.1")),
            ValidTestCase(generateTestIonValues(IonType.FLOAT), "12345e0", Text("12345e0")),
            ValidTestCase(generateTestIonValues(IonType.BOOL), "true", Text("true")),
            ValidTestCase(generateTestIonValues(IonType.TIMESTAMP),"2001-09-09T01:46:39.999Z", Text("2001-09-09T01:46:39.999Z")),
            ValidTestCase(generateTestIonValues(IonType.STRUCT), "{field:1}", Text("{field:1}")),
            ValidTestCase(generateTestIonValues(IonType.LIST), "[1]", Text("[1]")),
            ValidTestCase(generateTestIonValues(IonType.SEXP), "(1)", Text("(1)")),
    )

    private fun generateTestIonValues(type: IonType): IonValue {
        when (type) {
            IonType.STRING -> {
                return ION.newString("some string")
            }
            IonType.SYMBOL -> {
                return ION.newSymbol("some string")
            }
            IonType.DECIMAL -> {
                return ION.newDecimal(12.1)
            }
            IonType.FLOAT -> {
                return ION.newFloat(12345)
            }
            IonType.BOOL -> {
                return ION.newBool(true)
            }
            IonType.TIMESTAMP -> {
                return ION.newUtcTimestampFromMillis(999999999999)
            }
            IonType.STRUCT -> {
                val struct = ION.newEmptyStruct()
                struct.add("field", ION.newInt(1))
                return struct
            }
            IonType.LIST -> {
                val list = ION.newEmptyList()
                list.add(ION.newInt(1))
                return list
            }
            IonType.SEXP -> {
                val sexp = ION.newEmptySexp()
                sexp.add(ION.newInt(1))
                return sexp
            }
            else -> return ION.newNull()
        }
    }
}
