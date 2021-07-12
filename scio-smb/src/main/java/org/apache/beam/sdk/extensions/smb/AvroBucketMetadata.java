/*
 * Copyright 2019 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.smb;

import static org.apache.beam.sdk.coders.Coder.NonDeterministicException;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;

/**
 * {@link org.apache.beam.sdk.extensions.smb.BucketMetadata} for Avro {@link GenericRecord} records.
 */
public class AvroBucketMetadata<K, V extends GenericRecord> extends BucketMetadata<K, V> {

  @JsonProperty private final String keyField;

  @JsonIgnore private final String[] keyPath;

  @JsonProperty private final StringType stringProp;

  @JsonIgnore private final SerializableFunction<Object, K> keyCastFn;

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix,
      Class<V> recordClass)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        numBuckets,
        numShards,
        keyClass,
        hashType,
        keyField,
        filenamePrefix,
        new ReflectData(recordClass.getClassLoader()).getSchema(recordClass));
  }

  public AvroBucketMetadata(
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix,
      Schema schema)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        BucketMetadata.CURRENT_VERSION,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        AvroUtils.validateKeyField(keyField, keyClass, schema),
        filenamePrefix,
        AvroUtils.getStringTypeProp(keyField, schema));
  }

  AvroBucketMetadata(
      int version,
      int numBuckets,
      int numShards,
      Class<K> keyClass,
      BucketMetadata.HashType hashType,
      String keyField,
      String filenamePrefix)
      throws CannotProvideCoderException, NonDeterministicException {
    this(
        version,
        numBuckets,
        numShards,
        keyClass,
        hashType,
        keyField,
        filenamePrefix,
        StringType.Utf8);
  }

  @JsonCreator
  AvroBucketMetadata(
      @JsonProperty("version") int version,
      @JsonProperty("numBuckets") int numBuckets,
      @JsonProperty("numShards") int numShards,
      @JsonProperty("keyClass") Class<K> keyClass,
      @JsonProperty("hashType") BucketMetadata.HashType hashType,
      @JsonProperty("keyField") String keyField,
      @JsonProperty(value = "filenamePrefix", required = false) String filenamePrefix,
      @JsonProperty(value = "stringProp", required = false) StringType stringProp)
      throws CannotProvideCoderException, NonDeterministicException {
    super(version, numBuckets, numShards, keyClass, hashType, filenamePrefix);
    this.keyField = keyField;
    this.keyPath = AvroUtils.toKeyPath(keyField);
    this.stringProp = stringProp;
    this.keyCastFn = AvroUtils.getKeyCastFn(stringProp, keyClass);
  }

  @Override
  public Map<Class<?>, Coder<?>> coderOverrides() {
    return AvroUtils.coderOverrides();
  }

  @Override
  public K extractKey(V value) {
    GenericRecord node = value;
    for (int i = 0; i < keyPath.length - 1; i++) {
      node = (GenericRecord) node.get(keyPath[i]);
    }

    return keyCastFn.apply(node.get(keyPath[keyPath.length - 1]));
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("keyField", keyField));
  }

  @Override
  public boolean isPartitionCompatible(BucketMetadata o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroBucketMetadata<?, ?> that = (AvroBucketMetadata<?, ?>) o;
    return getKeyClass() == that.getKeyClass()
        && keyField.equals(that.keyField)
        && Arrays.equals(keyPath, that.keyPath);
  }
}
