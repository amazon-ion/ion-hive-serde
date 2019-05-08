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

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ion.IonValue
import com.amazon.ionhiveserde.ION
import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable

class IonNumberToDecimalObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonValue, HiveDecimalWritable, HiveDecimal>() {

    override fun validTestCases() = listOf(
            ValidTestCase(ION.newDecimal(1), HiveDecimal.ONE, HiveDecimalWritable(HiveDecimal.ONE)),
            ValidTestCase(ION.newDecimal(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO)),
            ValidTestCase(ION.newInt(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO)),
            ValidTestCase(ION.newInt(0), HiveDecimal.ZERO, HiveDecimalWritable(HiveDecimal.ZERO))
    )

    override val subject = com.amazon.ionhiveserde.objectinspectors.IonNumberToDecimalObjectInspector()
}

