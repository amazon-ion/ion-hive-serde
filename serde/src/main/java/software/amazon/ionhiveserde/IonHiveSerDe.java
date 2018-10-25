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

package software.amazon.ionhiveserde;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import software.amazon.ion.IonSystem;
import software.amazon.ion.IonWriter;
import software.amazon.ion.system.IonSystemBuilder;
import software.amazon.ionhiveserde.objectinspectors.factories.IonObjectInspectorFactory;

/**
 * <p>
 * Hive SerDe for the <a href="http://amzn.github.io/ion-docs/docs.html">Amazon Ion</a> data format.
 * </p>
 * <p>
 * For more information on Hive SerDes see <a href="https://cwiki.apache.org/confluence/display/Hive/SerDe">wiki</a>.
 * </p>
 */
public class IonHiveSerDe extends AbstractSerDe {

    private IonSystem ion;
    private ObjectInspector objectInspector;
    private SerDeProperties serDeProperties;
    private SerDeStats stats;

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("deprecation") // we are forced to override this constructor even though it's deprecated
    public void initialize(final @Nullable Configuration conf, final Properties properties) throws SerDeException {
        stats = new SerDeStats();
        final List<String> columnNames = readColumnNames(properties);
        final StructTypeInfo tableInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
            columnNames,
            readColumnTypes(properties));

        ion = buildIonSystem();

        serDeProperties = new SerDeProperties(properties, columnNames);

        objectInspector = IonObjectInspectorFactory.objectInspectorForTable(tableInfo, serDeProperties);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<? extends Writable> getSerializedClass() {
        return serDeProperties.getEncoding() == IonEncoding.BINARY ? BytesWritable.class : Text.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Writable serialize(final Object data, final ObjectInspector objectInspector) throws SerDeException {
        if (objectInspector.getCategory() != STRUCT) {
            throw new SerDeException("Can only serialize struct types, got: " + objectInspector.getTypeName());
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (final IonWriter writer = newWriter(out)) {
            Serializer.serializeStruct(writer, data, (StructObjectInspector) objectInspector, serDeProperties);
        } catch (IOException | IllegalArgumentException e) {
            throw new SerDeException(e);
        }

        return serDeProperties.getEncoding() == IonEncoding.BINARY
            ? new BytesWritable(out.toByteArray())
            : new Text(out.toByteArray());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object deserialize(final Writable blob) throws SerDeException {
        final byte[] bytes;
        final int length;

        // even though IonInputFormat only generates BytesWritable it's possible to use this SerDe with the default
        // TextFormat which produces Text
        if (blob instanceof Text) {
            Text text = (Text) blob;
            // getBytes returns a reference to the current buffer which is only valid up to length
            bytes = text.getBytes();
            length = text.getLength();

        } else if (blob instanceof BytesWritable) {
            BytesWritable bytesWritable = (BytesWritable) blob;
            // getBytes returns a reference to the current buffer which is only valid up to length
            bytes = bytesWritable.getBytes();
            length = bytesWritable.getLength();

        } else {
            throw new SerDeException("Invalid Writable instance, must be either Text or BytesWritable, was "
                + blob.getClass());
        }

        return Deserializer.deserialize(ion, bytes, length);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SerDeStats getSerDeStats() {
        return stats;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    private IonWriter newWriter(final OutputStream out) {
        return serDeProperties.getEncoding() == IonEncoding.BINARY ? ion.newBinaryWriter(out) : ion.newTextWriter(out);
    }

    private List<String> readColumnNames(final Properties tbl) {
        final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);

        if (columnNameProperty.isEmpty()) {
            return new ArrayList<>();
        }

        return Arrays.asList(columnNameProperty.split(","));
    }

    private List<TypeInfo> readColumnTypes(final Properties tbl) {
        final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

        if (columnTypeProperty.isEmpty()) {
            return new ArrayList<>();
        }

        return TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    }

    private IonSystem buildIonSystem() {
        // TODO configure it from SERDEPROPERTIES

        return IonSystemBuilder.standard().build();
    }
}

