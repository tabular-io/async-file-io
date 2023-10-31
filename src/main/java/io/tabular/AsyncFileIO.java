/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.tabular;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.Files;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.iceberg.shaded.com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iceberg.shaded.com.github.benmanes.caffeine.cache.LoadingCache;
import org.apache.iceberg.shaded.com.github.benmanes.caffeine.cache.RemovalListener;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/** A {@link FileIO} implementation that caches files when an {@link InputFile} is created. */
public class AsyncFileIO extends ResolvingFileIO {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncFileIO.class);

  private static final String FILE_PREFIX = "file:";
  private static final String ASYNC_ENABLED = "async.enabled";
  private static final String CACHE_LOCATION = "async.cache-location";

  // Using a holder instead of double-checked locking
  // see https://stackoverflow.com/questions/6189099/
  private static final class Shared {
    static final ExecutorService DOWNLOAD_POOL = ThreadPools.newWorkerPool("async-file-io");
    static final CacheTracker CACHE_TRACKER = new CacheTracker();
  }

  // identifies a FileIO instance for sharing a cache
  private final String uuid = UUID.randomUUID().toString();
  private SerializableMap<String, String> properties = null;
  private boolean enabled = true;

  private transient LoadingCache<InputFileKey, FutureInputFile> lazyCache = null;

  @Override
  public InputFile newInputFile(String path) {
    InputFile source = super.newInputFile(path);
    if (enabled) {
      return cache().get(key(source));
    }

    return source;
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return super.newOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    super.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return properties.immutableMap();
  }

  @Override
  public void initialize(Map<String, String> props) {
    super.initialize(props);

    this.properties = SerializableMap.copyOf(props);
    this.enabled = PropertyUtil.propertyAsBoolean(props, ASYNC_ENABLED, true);
  }

  @Override
  public void close() {
    if (lazyCache != null) {
      this.lazyCache = null;
      Shared.CACHE_TRACKER.release(this);
    }

    super.close();
  }

  private LoadingCache<InputFileKey, FutureInputFile> cache() {
    if (null == lazyCache) {
      this.lazyCache = Shared.CACHE_TRACKER.cacheFor(this);
    }

    return lazyCache;
  }

  private static String scheme(String location) {
    int colonPos = location.indexOf(":");
    if (colonPos > 0) {
      return location.substring(0, colonPos);
    }

    return null;
  }

  private static String cacheLocation(String configPath, String uuid) {
    String withoutPrefix;
    if (configPath.startsWith(FILE_PREFIX)) {
      withoutPrefix = configPath.substring(FILE_PREFIX.length());
    } else {
      withoutPrefix = configPath;
    }

    return String.format("%s/%s/%s", withoutPrefix, "async-file-io", uuid.substring(0, 8));
  }

  private static String cachePath(String cacheLocation, String unique) {
    return String.format("%s/%s/%s", cacheLocation, unique.substring(0, 2), unique.substring(2));
  }

  private static FileIO newLocalIO(String location, Map<String, String> properties) {
    FileIO local;
    String scheme = scheme(location);
    if (null == scheme || "file".equals(scheme)) {
      local = new LocalFileIO();
    } else if ("memory".equals(scheme)) {
      local = new InMemoryFileIO();
    } else {
      LOG.warn("Invalid location for async file cache: {}, using in-memory", location);
      local = new InMemoryFileIO();
    }

    local.initialize(properties);

    return local;
  }

  private static class LocalFileIO implements FileIO {
    @Override
    public InputFile newInputFile(String path) {
      return Files.localInput(path);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return Files.localOutput(path);
    }

    @Override
    public void deleteFile(String path) {
      try {
        new File(path).delete();
      } catch (RuntimeException e) {
        LOG.warn("Failed to delete path: {}", path, e);
      }
    }
  }

  private static final class CacheTracker implements Closeable {
    // keyed by AsyncFileIO UUID
    private final Map<String, Integer> refCounts = Maps.newHashMap();
    private final Map<String, LoadingCache<InputFileKey, FutureInputFile>> caches = Maps.newHashMap();
    private final Map<String, IOManager> ioManagers = Maps.newHashMap();

    synchronized LoadingCache<InputFileKey, FutureInputFile> cacheFor(AsyncFileIO io) {
      increment(io);
      return caches.computeIfAbsent(io.uuid, uuid -> newCache(ioManagers, io));
    }

    synchronized void release(AsyncFileIO io) {
      if (decrement(io) <= 0) {
        close(caches.get(io.uuid));
        ioManagers.get(io.uuid).close();
      }
    }

    @Override
    public synchronized void close() {
      refCounts.clear();
      caches.values().forEach(CacheTracker::close);
      caches.clear();
      ioManagers.values().forEach(IOManager::close);
      ioManagers.clear();
    }

    private void increment(AsyncFileIO io) {
      refCounts.put(io.uuid, refCounts.getOrDefault(io.uuid, 0) + 1);
    }

    private int decrement(AsyncFileIO io) {
      int count = refCounts.get(io.uuid) - 1;
      refCounts.put(io.uuid, count);
      return count;
    }

    private static LoadingCache<InputFileKey, FutureInputFile> newCache(Map<String, IOManager> ioManagers, AsyncFileIO io) {
      Map<String, String> props = io.properties();

      String baseLocation = props.getOrDefault(CACHE_LOCATION, System.getProperty("java.io.tmpdir"));
      String cacheLocation = cacheLocation(baseLocation, io.uuid);

      IOManager ioManager = ioManagers.computeIfAbsent(io.uuid, cacheLoc -> new IOManager(cacheLocation, newLocalIO(baseLocation, props)));

      return Caffeine.newBuilder()
              .softValues()
              .removalListener(
                      (RemovalListener<InputFileKey, FutureInputFile>) (path, cached, cause) -> ioManager.delete(path))
              .build(ioManager::download);
    }

    private static void close(LoadingCache<InputFileKey, FutureInputFile> cache) {
      if (cache != null) {
        cache.invalidateAll();
        cache.cleanUp();
      }
    }
  }

  private static InputFileKey key(InputFile source) {
    return new InputFileKey(source);
  }

  private static class InputFileKey implements InputFile {
    private final String uuid = UUID.randomUUID().toString();
    private final InputFile wrapped;

    public InputFileKey(InputFile wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public long getLength() {
      return wrapped.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return wrapped.newStream();
    }

    @Override
    public String location() {
      return wrapped.location();
    }

    @Override
    public boolean exists() {
      return wrapped.exists();
    }

    @Override
    public int hashCode() {
      return wrapped.location().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof InputFileKey)) {
        return false;
      }

      return wrapped.location().equals(((InputFileKey) obj).location());
    }

    @Override
    public String toString() {
      return wrapped.location();
    }
  }

  /**
   * A stateless manager for caching files in a FileIO. Provides helper methods but does not track files.
   */
  private static class IOManager implements Closeable {
    private final String cacheLocation;
    private final FileIO local;

    private IOManager(String cacheLocation, FileIO local) {
      this.cacheLocation = cacheLocation;
      this.local = local;
    }

    private void delete(InputFileKey source) {
      local.deleteFile(cachePath(cacheLocation, source.uuid));
    }

    private FutureInputFile download(InputFileKey source) {
      String cachedPath = cachePath(cacheLocation, source.uuid);

      Future<InputFile> downloadFuture =
              Shared.DOWNLOAD_POOL.submit(
                      () -> {
                        LOG.info("Starting download for {}", source);

                        OutputFile copy = local.newOutputFile(cachedPath);
                        try (InputStream in = source.newStream();
                             OutputStream out = copy.createOrOverwrite()) {
                          ByteStreams.copy(in, out);
                        } catch (IOException e) {
                          LOG.warn("Failed to download {} to {}", source, cachedPath, e);
                          return null;
                        }

                        LOG.info("Finished download for {}", source);

                        return copy.toInputFile();
                      });

      return new FutureInputFile(source, downloadFuture);
    }

    @Override
    public void close() {
      if (local instanceof LocalFileIO) {
        // remove the cache location
        try {
          FileUtils.deleteDirectory(new File(cacheLocation));
        } catch (RuntimeException | IOException e) {
          LOG.warn("Failed to delete cache location: {}", cacheLocation, e);
        }
      }
    }
  }

  private static class FutureInputFile implements InputFile {
    private final InputFile source;
    private final Future<InputFile> future;
    private InputFile delegate = null;

    private FutureInputFile(InputFile source, Future<InputFile> future) {
      this.source = source;
      this.future = future;
    }

    private InputFile delegate() {
      if (delegate == null) {
        try {
          this.delegate = future.get();
          if (delegate == null) {
            // This means the download failed
            this.delegate = source;
          } else {
            LOG.info("Using downloaded copy of {}", source.location());
          }
        } catch (ExecutionException e) {
          LOG.warn("Download failed", e);
          this.delegate = source;
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          this.delegate = source;
        }
      }

      return delegate;
    }

    @Override
    public long getLength() {
      // use the local copy if there is one
      if (future.isDone()) {
        return delegate().getLength();
      } else {
        return source.getLength();
      }
    }

    @Override
    public SeekableInputStream newStream() {
      if (future.isDone()) {
        return delegate().newStream();
      } else {
        LOG.info("Using remote copy of {}", source.location());
        return source.newStream();
      }
    }

    @Override
    public String location() {
      return source.location();
    }

    @Override
    public boolean exists() {
      return source.exists();
    }
  }
}
