package io.openmessaging.benchmark;

public interface WorkloadGeneratorInterface extends AutoCloseable {

    TestResult run() throws Exception;
}
