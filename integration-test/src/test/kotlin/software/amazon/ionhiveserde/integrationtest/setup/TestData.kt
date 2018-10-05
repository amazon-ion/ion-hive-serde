/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at:
 *
 *     http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 *
 */

package software.amazon.ionhiveserde.integrationtest.setup

import software.amazon.ion.IonStruct
import software.amazon.ionhiveserde.integrationtest.ION
import software.amazon.ionhiveserde.integrationtest.singleValueFromPath

/**
 * Holds DOM values for ion files located in test-data/
 */
object TestData {
    val typeMapping: IonStruct by lazy { ION.singleValueFromPath("test-data/type-mapping.ion") as IonStruct }
}
