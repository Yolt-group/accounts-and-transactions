package com.yolt.accountsandtransactions.legacytransactions;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public class LegacyTransactionsByAccountDTO {

    private List<LegacyTransactionDTO> transactions;

    @JsonProperty("_links")
    private LinksDTO links;

    @Getter
    @AllArgsConstructor
    public static class LinksDTO {

        private LinkDTO next;

//  These links were left out because they are app-specific. Assuming that B2B clients do not parse them.
//
//        private LinkDTO account;
//        private LinkDTO update;
//        private LinkDTO similarTransactions;
//        private LinkDTO similarTransactionsForBulkTagging;
    }
}
