package org.rmow.bench.io;

import java.util.Map;

/**
 * Counts the requests a cloud store's SDK sends ({@link S3Recorder}, {@link AzureRecorder}), so
 * each phase can record what reached the store beside what {@link TracingFileIO} modelled.
 */
public interface SdkRecorder {

  /**
   * The counts so far, as step-record fields ({@code sdk_*}). Every value is a running total, so
   * a phase's counts are the difference of two snapshots, field by field.
   */
  Map<String, Long> fields();
}
