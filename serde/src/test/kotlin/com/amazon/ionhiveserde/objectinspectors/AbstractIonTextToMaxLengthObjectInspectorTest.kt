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

package com.amazon.ionhiveserde.objectinspectors

import com.amazon.ionhiveserde.ION
import org.junit.Test

abstract class AbstractIonTextToMaxLengthObjectInspectorTest : AbstractIonPrimitiveJavaObjectInspectorTest() {

    companion object {
        const val maxLength = 10
        const val valid = "text"
        const val invalid = "text too long"
    }

    abstract override val subject: AbstractIonPrimitiveJavaObjectInspector

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveWritableObjectWithStringTooLong() {
        val string = ION.newString(invalid)

        subject.getPrimitiveWritableObject(string)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveWritableObjectWithSymbolTooLong() {
        val symbol = ION.newSymbol(invalid)

        subject.getPrimitiveWritableObject(symbol)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveJavaObjectWithStringTooLong() {
        val string = ION.newString(invalid)

        subject.getPrimitiveJavaObject(string)
    }

    @Test(expected = IllegalArgumentException::class)
    fun getPrimitiveJavaObjectWithSymbolTooLong() {
        val symbol = ION.newSymbol(invalid)

        subject.getPrimitiveJavaObject(symbol)
    }
}



