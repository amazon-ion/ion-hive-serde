// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package com.amazon.ionhiveserde;

import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.STRUCT;

import com.amazon.ion.IonException;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonStruct;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonWriter;
import com.amazon.ionhiveserde.caseinsensitivedecorator.IonStructCaseInsensitiveDecorator;
import com.amazon.ionhiveserde.configuration.IonEncoding;
import com.amazon.ionhiveserde.configuration.SerDeProperties;
import com.amazon.ionhiveserde.serializers.TableSerializer;
import com.amazon.ionpathextraction.PathExtractor;
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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * <p>
 * Hive SerDe for the <a href="https://amazon-ion.github.io/ion-docs/docs.html">Amazon Ion</a> data format.
 * </p>
 * <p>
 * For more information on Hive SerDes see <a href="https://cwiki.apache.org/confluence/display/Hive/SerDe">wiki</a>.
 * </p>
 */
public abstract class AbstractIonHiveSerDe extends AbstractSerDe {

    private ObjectInspector objectInspector;
    protected SerDeProperties serDeProperties;
    private SerDeStats stats;
    private TableSerializer serializer;
    private IonFactory ionFactory;

    protected abstract TableSerializer getTableSerializer(SerDeProperties properties);

    protected abstract ObjectInspector getObjectInspectorForTable(SerDeProperties properties);

    @Override
    // we are forced to override this constructor even though it's deprecated
    public final void initialize(final @Nullable Configuration conf, final Properties properties) {
        stats = new SerDeStats();
        final List<String> columnNames = readColumnNames(properties);
        final List<TypeInfo> columnTypes = readColumnTypes(properties);

        serDeProperties = new SerDeProperties(properties, columnNames, columnTypes);

        ionFactory = new IonFactory(serDeProperties);

        objectInspector = getObjectInspectorForTable(serDeProperties);
        serializer = getTableSerializer(serDeProperties);
    }

    @Override
    public final Class<? extends Writable> getSerializedClass() {
        return serDeProperties.getEncoding() == IonEncoding.BINARY
            ? BytesWritable.class
            : Text.class;
    }

    @Override
    public final Writable serialize(final Object data, final ObjectInspector objectInspector) throws SerDeException {
        if (objectInspector.getCategory() != STRUCT) {
            throw new SerDeException("Can only serialize struct types, got: " + objectInspector.getTypeName());
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (final IonWriter writer = newWriter(out)) {
            serializer.serialize(writer, data, objectInspector);
        } catch (IOException | IllegalArgumentException e) {
            throw new SerDeException(e);
        }

        return serDeProperties.getEncoding() == IonEncoding.BINARY
            ? new BytesWritable(out.toByteArray())
            : new Text(out.toByteArray());
    }

    @Override
    public final Object deserialize(final Writable blob) throws SerDeException {
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

        final IonSystem domFactory = ionFactory.getDomFactory();
        try (final IonReader reader = ionFactory.newReader(bytes, 0, length)) {

            /*
                We're using an IonStruct here because:
                1. We need a key-value store to carry column values
                2. The top-level IonStruct as a context object carries the IonSystem which we use as a ValueFactory in
                   the callbacks created in PathExtractionConfig
                Refer to https://github.com/amazon-ion/ion-hive-serde/issues/61.
            */
            IonStruct struct = domFactory.newEmptyStruct();
            if (!serDeProperties.pathExtractorCaseSensitivity()) {
                struct = new IonStructCaseInsensitiveDecorator(struct);
            }
            final PathExtractor<IonStruct> pathExtractor = serDeProperties.pathExtractor();

            pathExtractor.match(reader, struct);

            return struct;

        } catch (IonException e) {
            // skips if ignoring malformed
            if (serDeProperties.getIgnoreMalformed()) {
                return null;
            }

            throw e;
        } catch (IOException e) {
            throw new SerDeException(e);
        }
    }

    @Override
    public final SerDeStats getSerDeStats() {
        return stats;
    }

    @Override
    public final ObjectInspector getObjectInspector() throws SerDeException {
        return objectInspector;
    }

    private IonWriter newWriter(final OutputStream out) {
        return serDeProperties.getEncoding() == IonEncoding.BINARY
            ? ionFactory.newBinaryWriter(out)
            : ionFactory.newTextWriter(out);
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
}
