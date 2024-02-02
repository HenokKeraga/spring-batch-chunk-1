package com.example.springbatchchunk.config.partitioner;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SendStudentPartitioner implements Partitioner {
    private Map<String, ExecutionContext> partitioner;

    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {
        return partitioner;
    }

    private SendStudentPartitioner(Builder builder) {
        this.partitioner = builder.partitioner;
    }


    public static class Builder {
        public Map<String, ExecutionContext> partitioner;

        public Builder(String key, Set<?> partitionValues) {
            partitioner = new HashMap<>();
            for (var value : partitionValues) {
                var executionContext = new ExecutionContext();
                executionContext.put(key, value);
                partitioner.put(key+value,executionContext);
            }
        }

        public SendStudentPartitioner build() {
            return new SendStudentPartitioner(this);
        }


    }
}
