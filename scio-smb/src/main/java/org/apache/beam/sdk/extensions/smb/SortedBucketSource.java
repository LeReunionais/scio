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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.PartitionMetadata;
import org.apache.beam.sdk.extensions.smb.BucketMetadataUtil.SourceMetadata;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.fs.ResourceIdCoder;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Builder;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGbkResultSchema;
import org.apache.beam.sdk.transforms.join.UnionCoder;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Function;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.UnsignedBytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} for co-grouping sources written using compatible {@link SortedBucketSink}
 * transforms. It differs from {@link org.apache.beam.sdk.transforms.join.CoGroupByKey} because no
 * shuffle step is required, since the source files are written in pre-sorted order. Instead,
 * matching buckets' files are sequentially read in a merge-sort style, and outputs resulting value
 * groups as {@link org.apache.beam.sdk.transforms.join.CoGbkResult}.
 *
 * <h3>Source compatibility</h3>
 *
 * <p>Each of the {@link BucketedInput} sources must use the same key function and hashing scheme.
 * Since {@link SortedBucketSink} writes an additional file representing {@link BucketMetadata},
 * {@link SortedBucketSource} begins by reading each metadata file and using {@link
 * BucketMetadata#isCompatibleWith(BucketMetadata)} to check compatibility.
 *
 * <p>The number of buckets, {@code N}, does not have to match across sources. Since that value is
 * required be to a power of 2, all values of {@code N} are compatible, albeit requiring a fan-out
 * from the source with smallest {@code N}.
 *
 * @param <FinalKeyT> the type of the result keys. Sources can have different key types as long as
 *     they can all be decoded as this type (see: {@link BucketMetadata#getKeyCoder()} and are
 *     bucketed using the same {@code byte[]} representation (see: {@link
 *     BucketMetadata#getKeyBytes(Object)}.
 */
public class SortedBucketSource<FinalKeyT> extends BoundedSource<KV<FinalKeyT, CoGbkResult>> {

  // Dataflow calls split() with a suggested byte size that assumes a higher throughput than
  // SMB joins have. By adjusting this suggestion we can arrive at a more optimal parallelism.
  static final Double DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR = 0.5;

  private static final AtomicInteger metricsId = new AtomicInteger(1);

  private static final Comparator<byte[]> bytesComparator =
      UnsignedBytes.lexicographicalComparator();

  private static final Logger LOG = LoggerFactory.getLogger(SortedBucketSource.class);

  private final Class<FinalKeyT> finalKeyClass;
  private final List<BucketedInput<?, ?>> sources;
  private final TargetParallelism targetParallelism;
  private final int effectiveParallelism;
  private final int bucketOffsetId;
  private SourceSpec<FinalKeyT> sourceSpec;
  private final Distribution keyGroupSize;
  private Long estimatedSizeBytes;
  private final String metricsKey;

  public SortedBucketSource(Class<FinalKeyT> finalKeyClass, List<BucketedInput<?, ?>> sources) {
    this(finalKeyClass, sources, TargetParallelism.auto());
  }

  public SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(finalKeyClass, sources, targetParallelism, 0, 1, getDefaultMetricsKey());
  }

  public SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      String metricsKey) {
    // Initialize with absolute minimal parallelism and allow split() to create parallelism
    this(finalKeyClass, sources, targetParallelism, 0, 1, metricsKey);
  }

  private SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism,
      String metricsKey) {
    this(
        finalKeyClass,
        sources,
        targetParallelism,
        bucketOffsetId,
        effectiveParallelism,
        metricsKey,
        null);
  }

  private SortedBucketSource(
      Class<FinalKeyT> finalKeyClass,
      List<BucketedInput<?, ?>> sources,
      TargetParallelism targetParallelism,
      int bucketOffsetId,
      int effectiveParallelism,
      String metricsKey,
      Long estimatedSizeBytes) {
    this.finalKeyClass = finalKeyClass;
    this.sources = sources;
    this.targetParallelism = targetParallelism;
    this.bucketOffsetId = bucketOffsetId;
    this.effectiveParallelism = effectiveParallelism;
    this.metricsKey = metricsKey;
    this.keyGroupSize =
        Metrics.distribution(SortedBucketSource.class, metricsKey + "-KeyGroupSize");
    this.estimatedSizeBytes = estimatedSizeBytes;
  }

  private static String getDefaultMetricsKey() {
    final int nextMetricsId = metricsId.getAndAdd(1);
    if (nextMetricsId != 1) {
      return "SortedBucketSource{" + nextMetricsId + "}";
    } else {
      return "SortedBucketSource";
    }
  }

  @VisibleForTesting
  int getBucketOffset() {
    return bucketOffsetId;
  }

  @VisibleForTesting
  int getEffectiveParallelism() {
    return effectiveParallelism;
  }

  private SourceSpec<FinalKeyT> getOrComputeSourceSpec() {
    if (this.sourceSpec == null) {
      this.sourceSpec = SourceSpec.from(finalKeyClass, sources);
    }
    return this.sourceSpec;
  }

  @Override
  public Coder<KV<FinalKeyT, CoGbkResult>> getOutputCoder() {
    return KvCoder.of(
        getOrComputeSourceSpec().keyCoder,
        CoGbkResult.CoGbkResultCoder.of(
            BucketedInput.schemaOf(sources),
            UnionCoder.of(
                sources.stream().map(BucketedInput::getCoder).collect(Collectors.toList()))));
  }

  @Override
  public void populateDisplayData(Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("targetParallelism", targetParallelism.toString()));
    builder.add(DisplayData.item("keyClass", finalKeyClass.toString()));
    builder.add(DisplayData.item("metricsKey", metricsKey));
  }

  @Override
  public List<? extends BoundedSource<KV<FinalKeyT, CoGbkResult>>> split(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception {
    final int numSplits =
        getNumSplits(
            getOrComputeSourceSpec(),
            effectiveParallelism,
            targetParallelism,
            getEstimatedSizeBytes(options),
            desiredBundleSizeBytes,
            DESIRED_SIZE_BYTES_ADJUSTMENT_FACTOR);

    final long estSplitSize = estimatedSizeBytes / numSplits;

    final DecimalFormat sizeFormat = new DecimalFormat("0.00");
    LOG.info(
        "Parallelism was adjusted by {}splitting source of size {} MB into {} source(s) of size {} MB",
        effectiveParallelism > 1 ? "further " : "",
        sizeFormat.format(estimatedSizeBytes / 1000000.0),
        numSplits,
        sizeFormat.format(estSplitSize / 1000000.0));

    final int totalParallelism = numSplits * effectiveParallelism;
    return IntStream.range(0, numSplits)
        .boxed()
        .map(
            i ->
                new SortedBucketSource<>(
                    finalKeyClass,
                    sources,
                    targetParallelism,
                    bucketOffsetId + (i * effectiveParallelism),
                    totalParallelism,
                    metricsKey,
                    estSplitSize))
        .collect(Collectors.toList());
  }

  static int getNumSplits(
      SourceSpec sourceSpec,
      int effectiveParallelism,
      TargetParallelism targetParallelism,
      long estimatedSizeBytes,
      long desiredSizeBytes,
      double adjustmentFactor) {
    desiredSizeBytes *= adjustmentFactor;

    int greatestNumBuckets = sourceSpec.greatestNumBuckets;

    if (effectiveParallelism == greatestNumBuckets) {
      LOG.info("Parallelism is already maxed, can't split further.");
      return 1;
    }
    if (!targetParallelism.isAuto()) {
      return sourceSpec.getParallelism(targetParallelism);
    } else {
      int fanout = (int) Math.round(estimatedSizeBytes / (desiredSizeBytes * 1.0));

      if (fanout <= 1) {
        LOG.info("Desired byte size is <= total input size, can't split further.");
        return 1;
      }

      // round up to nearest power of 2, bounded by greatest # of buckets
      return Math.min(Integer.highestOneBit(fanout - 1) * 2, greatestNumBuckets);
    }
  }

  // `getEstimatedSizeBytes` is called frequently by Dataflow, don't recompute every time
  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    if (estimatedSizeBytes == null) {
      estimatedSizeBytes =
          sources.parallelStream().mapToLong(BucketedInput::getOrSampleByteSize).sum();

      LOG.info("Estimated byte size is " + estimatedSizeBytes);
    }

    return estimatedSizeBytes;
  }

  @Override
  public BoundedReader<KV<FinalKeyT, CoGbkResult>> createReader(PipelineOptions options)
      throws IOException {
    return new MergeBucketsReader<>(
        options,
        sources,
        bucketOffsetId,
        effectiveParallelism,
        getOrComputeSourceSpec(),
        this,
        keyGroupSize,
        true);
  }

  /** Merge key-value groups in matching buckets. */
  static class MergeBucketsReader<FinalKeyT> extends BoundedReader<KV<FinalKeyT, CoGbkResult>> {
    private final SortedBucketSource<FinalKeyT> currentSource;
    private final MultiSourceKeyGroupReader<FinalKeyT> iter;
    private KV<FinalKeyT, CoGbkResult> next = null;

    MergeBucketsReader(
        PipelineOptions options,
        List<BucketedInput<?, ?>> sources,
        Integer bucketId,
        int parallelism,
        SourceSpec<FinalKeyT> sourceSpec,
        SortedBucketSource<FinalKeyT> currentSource,
        Distribution keyGroupSize,
        boolean materializeKeyGroup) {
      this.currentSource = currentSource;
      this.iter =
          new MultiSourceKeyGroupReader<>(
              sources,
              sourceSpec,
              keyGroupSize,
              materializeKeyGroup,
              bucketId,
              parallelism,
              options);
    }

    @Override
    public boolean start() throws IOException {
      return advance();
    }

    @Override
    public KV<FinalKeyT, CoGbkResult> getCurrent() throws NoSuchElementException {
      if (next == null) throw new NoSuchElementException();
      return next;
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean advance() throws IOException {
      next = iter.readNext();
      return next != null;
    }

    @Override
    public void close() throws IOException {}

    @Override
    public BoundedSource<KV<FinalKeyT, CoGbkResult>> getCurrentSource() {
      return currentSource;
    }
  }

  static class TraversableOnceIterable<V> implements Iterable<V> {
    private final Iterator<V> underlying;
    private boolean called = false;

    TraversableOnceIterable(Iterator<V> underlying) {
      this.underlying = underlying;
    }

    @Override
    public Iterator<V> iterator() {
      Preconditions.checkArgument(
          !called,
          "CoGbkResult .iterator() can only be called once. To be re-iterable, it must be materialized as a List.");
      called = true;
      return underlying;
    }

    void ensureExhausted() {
      this.underlying.forEachRemaining(v -> {});
    }
  }
  /**
   * Abstracts a sorted-bucket input to {@link SortedBucketSource} written by {@link
   * SortedBucketSink}.
   *
   * @param <K> the type of the keys that values in a bucket are sorted with
   * @param <V> the type of the values in a bucket
   */
  public static class BucketedInput<K, V> implements Serializable {
    private static final Pattern BUCKET_PATTERN = Pattern.compile("(\\d+)-of-(\\d+)");

    private TupleTag<V> tupleTag;
    private String filenameSuffix;
    private FileOperations<V> fileOperations;
    private List<ResourceId> inputDirectories;
    private Predicate<V> predicate;
    private transient SourceMetadata<K, V> sourceMetadata;

    public BucketedInput(
        TupleTag<V> tupleTag,
        ResourceId inputDirectory,
        String filenameSuffix,
        FileOperations<V> fileOperations) {
      this(tupleTag, Collections.singletonList(inputDirectory), filenameSuffix, fileOperations);
    }

    public BucketedInput(
        TupleTag<V> tupleTag,
        List<ResourceId> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations) {
      this(tupleTag, inputDirectories, filenameSuffix, fileOperations, null);
    }

    public BucketedInput(
        TupleTag<V> tupleTag,
        List<ResourceId> inputDirectories,
        String filenameSuffix,
        FileOperations<V> fileOperations,
        Predicate<V> predicate) {
      inputDirectories.forEach(
          path ->
              Preconditions.checkArgument(
                  path.isDirectory(),
                  "Cannot construct SMB source from non-directory input " + path));
      this.tupleTag = tupleTag;
      this.filenameSuffix = filenameSuffix;
      this.fileOperations = fileOperations;
      this.inputDirectories = inputDirectories;
      this.predicate = predicate;
    }

    public TupleTag<V> getTupleTag() {
      return tupleTag;
    }

    public Predicate<V> getPredicate() {
      return predicate;
    }

    public Coder<V> getCoder() {
      return fileOperations.getCoder();
    }

    static CoGbkResultSchema schemaOf(List<BucketedInput<?, ?>> sources) {
      return CoGbkResultSchema.of(
          sources.stream().map(BucketedInput::getTupleTag).collect(Collectors.toList()));
    }

    public BucketMetadata<K, V> getMetadata() {
      return getOrComputeMetadata().getCanonicalMetadata();
    }

    Map<ResourceId, PartitionMetadata> getPartitionMetadata() {
      return getOrComputeMetadata().getPartitionMetadata();
    }

    private SourceMetadata<K, V> getOrComputeMetadata() {
      if (sourceMetadata == null) {
        sourceMetadata =
            BucketMetadataUtil.get().getSourceMetadata(inputDirectories, filenameSuffix);
      }
      return sourceMetadata;
    }

    private static List<Metadata> sampleDirectory(ResourceId directory, String filepattern) {
      try {
        return FileSystems.match(
                directory.resolve(filepattern, StandardResolveOptions.RESOLVE_FILE).toString())
            .metadata();
      } catch (FileNotFoundException e) {
        return Collections.emptyList();
      } catch (IOException e) {
        throw new RuntimeException("Exception fetching metadata for " + directory, e);
      }
    }

    long getOrSampleByteSize() {
      return inputDirectories
          .parallelStream()
          .mapToLong(
              dir -> {
                // Take at most 10 buckets from the directory to sample
                // Check for single-shard filenames template first, then multi-shard
                List<Metadata> sampledFiles =
                    sampleDirectory(dir, "*-0000?-of-?????" + filenameSuffix);
                if (sampledFiles.isEmpty()) {
                  sampledFiles =
                      sampleDirectory(dir, "*-0000?-of-*-shard-00000-of-?????" + filenameSuffix);
                }

                int numBuckets = 0;
                long sampledBytes = 0L;
                final Set<String> seenBuckets = new HashSet<>();

                for (Metadata metadata : sampledFiles) {
                  final Matcher matcher =
                      BUCKET_PATTERN.matcher(metadata.resourceId().getFilename());
                  if (!matcher.find()) {
                    throw new RuntimeException(
                        "Couldn't match bucket information from filename: "
                            + metadata.resourceId().getFilename());
                  }
                  seenBuckets.add(matcher.group(1));
                  if (numBuckets == 0) {
                    numBuckets = Integer.parseInt(matcher.group(2));
                  }
                  sampledBytes += metadata.sizeBytes();
                }
                if (numBuckets == 0) {
                  throw new IllegalArgumentException("Directory " + dir + " has no bucket files");
                }
                if (seenBuckets.size() < numBuckets) {
                  return (long) (sampledBytes * (numBuckets / (seenBuckets.size() * 1.0)));
                } else {
                  return sampledBytes;
                }
              })
          .sum();
    }

    KeyGroupIterator<byte[], V> createIterator(
        int bucketId, int targetParallelism, PipelineOptions options) {
      final SortedBucketOptions opts = options.as(SortedBucketOptions.class);
      final int bufferSize = opts.getSortedBucketReadBufferSize();
      final int diskBufferMb = opts.getSortedBucketReadDiskBufferMb();
      FileOperations.setDiskBufferMb(diskBufferMb);
      final List<Iterator<V>> iterators =
          mapBucketFiles(
              bucketId,
              targetParallelism,
              file -> {
                try {
                  Iterator<V> iterator = fileOperations.iterator(file);
                  return bufferSize > 0 ? new BufferedIterator<>(iterator, bufferSize) : iterator;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      BucketMetadata<K, V> canonicalMetadata = sourceMetadata.getCanonicalMetadata();
      return new KeyGroupIterator<>(iterators, canonicalMetadata::getKeyBytes, bytesComparator);
    }

    private <T> List<T> mapBucketFiles(
        int bucketId, int targetParallelism, Function<ResourceId, T> mapFn) {
      final List<T> results = new ArrayList<>();
      getPartitionMetadata()
          .forEach(
              (resourceId, partitionMetadata) -> {
                final int numBuckets = partitionMetadata.getNumBuckets();
                final int numShards = partitionMetadata.getNumShards();

                for (int i = (bucketId % numBuckets); i < numBuckets; i += targetParallelism) {
                  for (int j = 0; j < numShards; j++) {
                    results.add(
                        mapFn.apply(
                            partitionMetadata
                                .getFileAssignment()
                                .forBucket(BucketShardId.of(i, j), numBuckets, numShards)));
                  }
                }
              });
      return results;
    }

    @Override
    public String toString() {
      return String.format(
          "BucketedInput[tupleTag=%s, inputDirectories=[%s]]",
          tupleTag.getId(),
          inputDirectories.size() > 5
              ? inputDirectories.subList(0, 4)
                  + "..."
                  + inputDirectories.get(inputDirectories.size() - 1)
              : inputDirectories);
    }

    // Not all instance members can be natively serialized, so override writeObject/readObject
    // using Coders for each type
    @SuppressWarnings("unchecked")
    private void writeObject(ObjectOutputStream outStream) throws IOException {
      SerializableCoder.of(TupleTag.class).encode(tupleTag, outStream);
      ListCoder.of(ResourceIdCoder.of()).encode(inputDirectories, outStream);
      outStream.writeUTF(filenameSuffix);
      outStream.writeObject(fileOperations);
      outStream.writeObject(predicate);
      outStream.flush();
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream inStream) throws ClassNotFoundException, IOException {
      this.tupleTag = SerializableCoder.of(TupleTag.class).decode(inStream);
      this.inputDirectories = ListCoder.of(ResourceIdCoder.of()).decode(inStream);
      this.filenameSuffix = inStream.readUTF();
      this.fileOperations = (FileOperations<V>) inStream.readObject();
      this.predicate = (Predicate<V>) inStream.readObject();
    }
  }

  /**
   * Filter predicate when building the {{@code Iterable<T>}} in {{@link CoGbkResult}}.
   *
   * <p>First argument {{@code List<T>}} is the work in progress buffer for the current key. Second
   * argument {{@code T}} is the next element to be added. Return true to accept the next element,
   * or false to reject it.
   */
  public interface Predicate<T> extends BiFunction<List<T>, T, Boolean>, Serializable {}
}
