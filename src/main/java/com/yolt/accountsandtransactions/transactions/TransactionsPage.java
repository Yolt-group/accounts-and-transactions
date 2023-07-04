package com.yolt.accountsandtransactions.transactions;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class TransactionsPage {
    private List<Transaction> transactions;
    private String next;
}
