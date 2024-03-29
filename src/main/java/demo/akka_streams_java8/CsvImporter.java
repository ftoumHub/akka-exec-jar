package demo.akka_streams_java8;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.util.ByteString;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import demo.akka_streams_java8.model.Reading;
import demo.akka_streams_java8.model.Reading.InvalidReading;
import demo.akka_streams_java8.model.Reading.ValidReading;
import demo.akka_streams_java8.util.Balancer;
import io.vavr.Tuple0;
import io.vavr.collection.List;
import io.vavr.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.stream.javadsl.FramingTruncation.ALLOW;
import static io.vavr.API.List;
import static io.vavr.API.println;
import static java.lang.System.currentTimeMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class CsvImporter {

    private static final Logger logger = LoggerFactory.getLogger(CsvImporter.class);

    private final ActorSystem system;

    private final File importDirectory;
    private final int linesToSkip;
    private final int concurrentFiles;
    private final int concurrentWrites;
    private final int nonIOParallelism;

    private CsvImporter(Config config, ActorSystem system) {
        this.system = system;
        this.importDirectory = Paths.get(config.getString("importer.import-directory")).toFile();
        this.linesToSkip = config.getInt("importer.lines-to-skip");
        this.concurrentFiles = config.getInt("importer.concurrent-files");
        this.concurrentWrites = config.getInt("importer.concurrent-writes");
        this.nonIOParallelism = config.getInt("importer.non-io-parallelism");
    }

    private CompletionStage<Reading> parseLine(String line) {
        return CompletableFuture.supplyAsync(() -> {
            String[] fields = line.split(";");
            int id = Integer.parseInt(fields[0]);

            return Try
                    .of(() -> Double.parseDouble(fields[1]))
                    .map(value -> (Reading) new ValidReading(id, value))
                    .onFailure(e -> logger.error("Unable to parse line: {}: {}", line, e.getMessage()))
                    .getOrElse(new InvalidReading(id));
        });
    }

    private Flow<ByteString, ByteString, NotUsed> lineDelimiter = Framing.delimiter(ByteString.fromString("\n"), 128, ALLOW);

    private Flow<File, Reading, NotUsed> parseFile() {
        return Flow.of(File.class).flatMapConcat(file -> {
            FileInputStream inputStream = new FileInputStream(file);
            return StreamConverters.fromInputStream(() -> inputStream)
                    .via(lineDelimiter)
                    .drop(linesToSkip)
                    .map(ByteString::utf8String)
                    .mapAsync(nonIOParallelism, this::parseLine);
        });
    }

    private Flow<Reading, ValidReading, NotUsed> computeAverage() {
        return Flow.of(Reading.class).grouped(2).mapAsyncUnordered(nonIOParallelism, readings ->
                CompletableFuture.supplyAsync(() -> {
                    List<ValidReading> validReadings = List.ofAll(readings)
                            .filter(ValidReading.class::isInstance)
                            .map(ValidReading.class::cast);

                    double average = validReadings.map(ValidReading::getValue).average().getOrElse(-1.0);

                    return new ValidReading(readings.get(0).getId(), average);
                }));
    }

    private Sink<ValidReading, CompletionStage<Done>> storeReadings() {
        return Flow.of(ValidReading.class)
                .mapAsyncUnordered(concurrentWrites, vr -> {
                    println(vr);
                    return completedFuture(Tuple0.instance());
                })
                .toMat(Sink.ignore(), Keep.right());
    }

    private Flow<File, ValidReading, NotUsed> processSingleFile() {
        return Flow.of(File.class).via(parseFile()).via(computeAverage());
    }

    private CompletionStage<Done> importFromFiles() {
        List<File> files = List(requireNonNull(importDirectory.listFiles()));
        logger.info("Starting import of {} files from {}", files.size(), importDirectory.getPath());

        long startTime = currentTimeMillis();

        Graph<FlowShape<File, ValidReading>, NotUsed> balancer = Balancer.create(concurrentFiles, processSingleFile());

        return Source.from(files)
                .via(balancer)
                .runWith(storeReadings(), ActorMaterializer.create(system))
                .whenComplete((d, e) -> {
                    if (d != null) {
                        logger.info("Import finished in {}s", (currentTimeMillis() - startTime) / 1000.0);
                    } else {
                        logger.error("Import failed", e);
                    }
                });
    }

    public static void main(String[] args) {
        Config config = ConfigFactory.load();
        ActorSystem system = ActorSystem.create();

        new CsvImporter(config, system)
                .importFromFiles()
                .thenAccept(d -> system.terminate());
    }
}
