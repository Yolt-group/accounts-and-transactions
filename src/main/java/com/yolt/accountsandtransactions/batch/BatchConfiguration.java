package com.yolt.accountsandtransactions.batch;

import org.springframework.boot.task.TaskExecutorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class BatchConfiguration {

    @Bean("batchJobCheckOldPendingTransactionsRunner")
    public ThreadPoolTaskExecutor BatchJobCheckOldPendingTransactionsRunner(TaskExecutorBuilder builder) {
        return builder
                .allowCoreThreadTimeOut(true)
                .corePoolSize(1)
                .maxPoolSize(1)
                .queueCapacity(0)
                .threadNamePrefix("BatchJobCheckOldPendingTransactionsRunner-")
                .build();
    }

    @Bean("BatchJobSyncTransactionTablesRunner")
    public ThreadPoolTaskExecutor BatchJobSyncTransactionTablesRunner(TaskExecutorBuilder builder) {
        return builder
                .allowCoreThreadTimeOut(true)
                .corePoolSize(1)
                .maxPoolSize(1)
                .queueCapacity(0)
                .threadNamePrefix("BatchJobSyncTransactionTablesRunner-")
                .build();
    }

    @Bean("BatchDeleteTransactionsOlderThanOneYear")
    public ThreadPoolTaskExecutor BatchDeleteTransactionsOlderThanOneYear(TaskExecutorBuilder builder) {
        return builder
                .allowCoreThreadTimeOut(true)
                .corePoolSize(1)
                .maxPoolSize(1)
                .queueCapacity(0)
                .threadNamePrefix("BatchDeleteTransactionsOlderThanOneYear-")
                .build();
    }


    @Bean("BatchPushDataToOffloadTopic")
    public ThreadPoolTaskExecutor BatchPushDataToOffloadTopic(TaskExecutorBuilder builder) {
        return builder
                .allowCoreThreadTimeOut(true)
                .corePoolSize(1)
                .maxPoolSize(1)
                .queueCapacity(0)
                .threadNamePrefix("BatchPushDataToOffloadTopic-")
                .build();
    }
}
