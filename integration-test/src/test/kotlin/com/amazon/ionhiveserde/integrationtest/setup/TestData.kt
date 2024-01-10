// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde.integrationtest.setup

import com.amazon.ion.IonStruct
import com.amazon.ionhiveserde.integrationtest.DOM_FACTORY
import com.amazon.ionhiveserde.integrationtest.singleValueFromPath

/**
 * Holds DOM values for ion files located in test-data/.
 */
object TestData {

    /** contents of type-mapping.ion as an [IonStruct]. */
    val typeMapping: IonStruct by lazy { DOM_FACTORY.singleValueFromPath("test-data/type-mapping.ion") as IonStruct }
}
