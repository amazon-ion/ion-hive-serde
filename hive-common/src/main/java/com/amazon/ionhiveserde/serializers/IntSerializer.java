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

package com.amazon.ionhiveserde.serializers;

import com.amazon.ion.IonType;
import com.amazon.ion.IonWriter;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.*;

import java.io.IOException;

/**
 * Int serializer.
 */
class IntSerializer implements IonSerializer {

    @Override
    public void serialize(final IonWriter writer,
                          final Object data,
                          final ObjectInspector objectInspector) throws IOException {

        final PrimitiveCategory category = ((PrimitiveObjectInspector) objectInspector).getPrimitiveCategory();
        switch (category) {
            case BYTE:
                writer.writeInt(((ByteObjectInspector) objectInspector).get(data));
                break;

            case SHORT:
                writer.writeInt(((ShortObjectInspector) objectInspector).get(data));
                break;

            case INT:
                writer.writeInt(((IntObjectInspector) objectInspector).get(data));
                break;

            case LONG:
                writer.writeInt(((LongObjectInspector) objectInspector).get(data));
                break;

            case DECIMAL:
                final HiveDecimal hiveDecimal =
                    ((HiveDecimalObjectInspector) objectInspector).getPrimitiveJavaObject(data);

                writer.writeInt(hiveDecimal.bigDecimalValue().toBigInteger());
                break;

            default:
                throw new IllegalArgumentException("Invalid object category for int serializer: " + category);
        }
    }

    @Override
    public IonType getIonType() {
        return IonType.INT;
    }
}
