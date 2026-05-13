package io.github.leonardsibelius.orders;

import org.apache.camel.main.Main;

public final class PipelineApplication {

    private PipelineApplication() {
    }

    public static void main(String[] args) throws Exception {
        Main main = new Main(PipelineApplication.class);
        main.run(args);
    }
}
