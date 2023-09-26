# async-file-io

An implementation of Apache Iceberg's FileIO that downloads files asynchronously.

Async downloads are started when a new `InputFile` is created from the `FileIO` instance. The `InputFile` returned will block when `newStream` is called until the download completes.

The underlying `ResolvingFileIO` is used for `newOutputFile` and `deleteFile`.

## Building

To build, run gradle build:

```
./gradlew build
```

## Configuration

To configure this `FileIO`, set the `io-impl` property on a catalog.

Here is an example of Spark configuration for a catalog named `prod`:

```
spark.sql.catalog.prod=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.prod.type=rest
spark.sql.catalog.prod.uri=https://api.tabular.io/ws
spark.sql.catalog.prod.credential=...
spark.sql.catalog.prod.warehouse=prod
spark.sql.catalog.prod.io-impl=io.tabular.AsyncFileIO
spark.sql.catalog.prod.async.cache-location=file:/tmp
```

Where data is locally stored is configured by `async.cache-location`. The cache location can be either a local path (e.g. `file:/tmp`) or `memory:/` to cache data in an in-memory `FileIO`.

To configure the number of background threads, set the Java system property `iceberg.worker.num-threads`.
