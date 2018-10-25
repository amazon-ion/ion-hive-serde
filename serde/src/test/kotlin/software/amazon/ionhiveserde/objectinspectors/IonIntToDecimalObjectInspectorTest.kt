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

import org.apache.hadoop.hive.common.type.HiveDecimal
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
import software.amazon.ion.IonInt
import software.amazon.ionhiveserde.ION
import java.math.BigDecimal

class IonIntToDecimalObjectInspectorTest
    : AbstractIonPrimitiveJavaObjectInspectorTest<IonInt, HiveDecimalWritable, HiveDecimal>() {

    override val subject = IonIntToDecimalObjectInspector()
    override fun validTestCases() = listOf(
            BigDecimal.ONE,
            BigDecimal.TEN
    ).map { ValidTestCase(ION.newInt(it), HiveDecimal.create(it), HiveDecimalWritable(HiveDecimal.create(it))) }
}