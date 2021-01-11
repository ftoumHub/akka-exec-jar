package demo.akka_streams_java8.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

public interface Reading {

    int getId();

    @AllArgsConstructor
    @Getter
    @ToString
    class InvalidReading implements Reading {

        private final int id;

    }

    @AllArgsConstructor
    @Getter
    @ToString
    class ValidReading implements Reading {

        private final int id;
        private final double value;

    }
}
