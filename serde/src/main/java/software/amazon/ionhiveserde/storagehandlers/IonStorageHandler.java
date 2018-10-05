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

package software.amazon.ionhiveserde.storagehandlers;

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import software.amazon.ionhiveserde.IonHiveSerDe;
import software.amazon.ionhiveserde.formats.IonInputFormat;

/**
 * Handles both Ion text and binary and should be used by any table based on Ion data.
 */
public class IonStorageHandler implements HiveStorageHandler {

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return IonInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        // TODO https://github.com/amzn/ion-hive-serde/issues/22
        return TextOutputFormat.class;
    }

    @Override
    public Class<IonHiveSerDe> getSerDeClass() {
        return IonHiveSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        // no hook
        return null;
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }

    @Override
    public void configureInputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        // do nothing by default.
    }

    @Override
    public void configureOutputJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        // do nothing by default.
    }

    @Override
    @SuppressWarnings("deprecation") // Deprecated method
    public void configureTableJobProperties(final TableDesc tableDesc, final Map<String, String> jobProperties) {
        // do nothing by default.
    }

    @Override
    public void configureJobConf(final TableDesc tableDesc, final JobConf jobConf) {
        // TODO hook in the serde props here as they are introduced
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(final Configuration conf) {
        this.conf = conf;
    }
}
