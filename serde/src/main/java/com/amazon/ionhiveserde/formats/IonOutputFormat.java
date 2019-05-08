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

package com.amazon.ionhiveserde.formats;

import com.amazon.ionhiveserde.IonHiveSerDe;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * Output format used in conjunction with the {@link IonHiveSerDe}. Handles both the Ion
 * text or Ion binary serialized by the SerDe
 * </p>
 *
 * <p>
 * <strong>WARNING:</strong> Must be used with {@link IonHiveSerDe}.
 * </p>
 */
public class IonOutputFormat extends FileOutputFormat<Object, Writable> implements HiveOutputFormat<Object, Writable> {

    @Override
    public RecordWriter<Object, Writable> getRecordWriter(final FileSystem ignored,
                                                          final JobConf job,
                                                          final String name,
                                                          final Progressable progress)
        throws IOException {

        final Path path = FileOutputFormat.getTaskOutputPath(job, name);
        final FileSystem fs = path.getFileSystem(job);
        final FSDataOutputStream fileOut = fs.create(path, progress);

        return new HadoopAdapter(new IonRecordWriter(fileOut));
    }

    @Override
    public FileSinkOperator.RecordWriter getHiveRecordWriter(final JobConf jc,
                                                             final Path finalOutPath,
                                                             final Class<? extends Writable> valueClass,
                                                             final boolean isCompressed,
                                                             final Properties tableProperties,
                                                             final Progressable progress)
        throws IOException {

        final FileSystem fs = finalOutPath.getFileSystem(jc);
        final OutputStream out = fs.create(finalOutPath, progress);

        return new IonRecordWriter(out);
    }

    private static class IonRecordWriter implements FileSinkOperator.RecordWriter {

        private final OutputStream out;

        IonRecordWriter(final OutputStream out) {
            this.out = out;
        }

        @Override
        public void write(final Writable value) throws IOException {
            boolean nullValue = value == null || value instanceof NullWritable;
            if (nullValue) {
                return;
            }

            // The SerDe already serialized the data to a Writable as either Ion binary or text. The output format
            // only needs to flush those bytes out to the destination

            if (value instanceof Text) {
                final Text text = (Text) value;
                out.write(text.getBytes(), 0, text.getLength());

            } else if (value instanceof BytesWritable) {
                final BytesWritable bytesWritable = (BytesWritable) value;
                out.write(bytesWritable.getBytes(), 0, bytesWritable.getLength());

            } else {
                throw new IllegalArgumentException("Unknown writable type: " + value.getClass());
            }
        }

        @Override
        public void close(final boolean abort) throws IOException {
            close();
        }

        void close() throws IOException {
            out.close();
        }
    }

    private static class HadoopAdapter implements RecordWriter<Object, Writable> {

        private final IonRecordWriter recordWriter;

        private HadoopAdapter(final IonRecordWriter recordWriter) {
            this.recordWriter = recordWriter;
        }

        @Override
        public void write(final Object key, final Writable value) throws IOException {
            recordWriter.write(value);
        }

        @Override
        public void close(final Reporter reporter) throws IOException {
            recordWriter.close();
        }
    }
}
