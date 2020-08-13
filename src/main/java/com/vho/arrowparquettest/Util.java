package com.vho.arrowparquettest;

import java.util.concurrent.ThreadLocalRandom;

public class Util {

    public static <T> T pickRandom(T[] options) {
        return options[ThreadLocalRandom.current().nextInt(0, options.length)];
    }
}
