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
import software.amazon.ion.IonText
import software.amazon.ionhiveserde.ION
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class IonTextToVarcharObjectInspectorTest
    : AbstractOverflowablePrimitiveObjectInspectorTest<IonText, HiveVarcharWritable, HiveVarchar>() {

    override val subject = IonTextToVarcharObjectInspector(5, true)
    override fun validTestCases() = listOf(
            ValidTestCase(ION.newString("1234"), HiveVarchar("1234", 5), HiveVarcharWritable(HiveVarchar("1234", 5))),
            ValidTestCase(ION.newSymbol("1234"), HiveVarchar("1234", 5), HiveVarcharWritable(HiveVarchar("1234", 5)))
    )

    override val subjectOverflow = IonTextToVarcharObjectInspector(5, false)

    override fun overflowTestCases() = listOf(
            OverflowTestCase(ION.newString("1234567"), HiveVarchar("12345", 5), HiveVarcharWritable(HiveVarchar("12345", 5))),
            OverflowTestCase(ION.newSymbol("1234567"), HiveVarchar("12345", 5), HiveVarcharWritable(HiveVarchar("12345", 5)))
    )
}