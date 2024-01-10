// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde;

import com.amazon.ionhiveserde.configuration.SerDeProperties;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToDateObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.IonTimestampToTimestampObjectInspector;
import com.amazon.ionhiveserde.objectinspectors.factories.IonObjectInspectorFactory;
import com.amazon.ionhiveserde.serializers.Hive2IonSerializerFactory;
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
                new Hive2IonSerializerFactory(),
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
