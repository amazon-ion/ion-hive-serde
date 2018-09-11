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

package software.amazon.ionhiveserde.objectinspectors;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import software.amazon.ion.IonText;
import software.amazon.ion.IonValue;

/**
 * Adapts an {@link IonText} for the varchar Hive type.
 */
public class IonTextToVarcharObjectInspector extends AbstractIonPrimitiveJavaObjectInspector implements
    HiveVarcharObjectInspector {

    private static final int DEFAULT_LENGTH = HiveVarchar.MAX_VARCHAR_LENGTH;

    private final int length;
    private final TextMaxLengthValidator validator = new TextMaxLengthValidator();

    public IonTextToVarcharObjectInspector() {
        this(DEFAULT_LENGTH);
    }

    /**
     * Creates an IonText to varchar with a maximum length.
     *
     * @param length max length
     */
    public IonTextToVarcharObjectInspector(final int length) {
        super(TypeInfoFactory.getVarcharTypeInfo(length));

        this.length = length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HiveVarcharWritable getPrimitiveWritableObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return new HiveVarcharWritable(getPrimitiveJavaObject((IonText) o));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HiveVarchar getPrimitiveJavaObject(final Object o) {
        if (IonUtil.isIonNull((IonValue) o)) {
            return null;
        }

        return getPrimitiveJavaObject((IonText) o);
    }

    private HiveVarchar getPrimitiveJavaObject(final IonText ionValue) {
        String text = ionValue.stringValue();
        return new HiveVarchar(validator.validate(text, length), length);
    }
}
