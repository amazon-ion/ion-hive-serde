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

package com.amazon.ionhiveserde;

import com.amazon.ionhiveserde.configuration.SerDeProperties;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToTimestampObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.factories.IonObjectInspectorFactory;
import com.amazon.ionhiveserde.serializers.Hive3IonSerializerFactory;
import com.amazon.ionhiveserde.serializers.TableSerializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * <p>
 * Hive SerDe for the <a href="https://amazon-ion.github.io/ion-docs/docs.html">Amazon Ion</a> data format.
 * </p>
 * <p>
 * For more information on Hive SerDes see <a href="https://cwiki.apache.org/confluence/display/Hive/SerDe">wiki</a>.
 * </p>
 */
public class IonHiveSerDe extends AbstractIonHiveSerDe {

    private static final IonObjectInspectorFactory ION_OBJECT_INSPECTOR_FACTORY =
            new IonObjectInspectorFactory(new IonTimestampToDateObjectInspector(),
                    new IonTimestampToTimestampObjectInspector());

    @Override
    protected TableSerializer getTableSerializer(final SerDeProperties properties) {
        return new TableSerializer(
                new Hive3IonSerializerFactory(),
                properties.getColumnNames(),
                properties
        );
    }

    @Override
    protected ObjectInspector getObjectInspectorForTable(final SerDeProperties properties) {
        final StructTypeInfo tableInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
                serDeProperties.getColumnNames(),
                serDeProperties.getColumnTypes());
        return ION_OBJECT_INSPECTOR_FACTORY.objectInspectorForTable(tableInfo, serDeProperties);
    }
}
