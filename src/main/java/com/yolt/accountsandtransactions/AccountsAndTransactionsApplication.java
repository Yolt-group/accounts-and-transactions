package com.yolt.accountsandtransactions;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import reactor.tools.agent.ReactorDebugAgent;

@EnableAsync
@SpringBootApplication
public class AccountsAndTransactionsApplication {

    // Install the bytecode instrumentation for the reactor debug hook
    static {
        ReactorDebugAgent.init();
    }

    public static void main(String[] args) {
        SpringApplication.run(AccountsAndTransactionsApplication.class, args);
    }
}
