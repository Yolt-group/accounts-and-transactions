package com.yolt.accountsandtransactions.legacytransactions;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Currency;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

@Getter
@ToString(onlyExplicitlyIncluded = true)
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Slf4j
public class LegacyTransactionDTO {
    @ToString.Include
    @NonNull
    private UUID accountId;

    @ToString.Include
    @NonNull
    private String id;

    @ToString.Include
    @NonNull
    private LegacyTransactionType transactionType;

    @ToString.Include
    @NonNull
    private LocalDate date;

    private ZonedDateTime dateTime;

    private BigDecimal amount;

    private Currency currency;

    private String category;

    private String description;

    private String shortDescription;

    private LegacyMerchantDTO merchantObject;

// This attribute was left out because they are app-specific.
//
//    private Note note;

    private UUID cycleId;

    private Set<String> labels;

    private Map<String, String> bankSpecific;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private ExtendedTransactionDTO extendedTransaction;

// These links were left out because they are app-specific. Assuming that B2B clients do not parse them.
//

//    @JsonProperty("_links")
//    private LegacyTransactionDTO.TransactionLinksDTO links;

//    @Data
//    @AllArgsConstructor
//    @NoArgsConstructor
//    @Builder
//    @JsonIgnoreProperties(ignoreUnknown = true)
//    @SuppressWarnings("WeakerAccess")
//    @ApiModel(value = "TransactionLinks", description = "Links related to a Transaction (HATEOAS)")
//    public static class TransactionLinksDTO {
//
//        @ApiModelProperty(value = "Link to self", required = true)
//        @JsonProperty("_self")
//        private LinkDTO self;
//
//        @ApiModelProperty(value = "Link to search query for similar transactions for recategorization", required = true)
//        private LinkDTO similarTransactionsForRecategorization;
//
//        @ApiModelProperty(value = "Link to search query for similar transactions for merchant update", required = true)
//        private LinkDTO similarTransactionsBasedOnMerchant;
//
//        @ApiModelProperty(value = "Link to search query for similar transactions for bulk tagging", required = true)
//        private LinkDTO similarTransactionsForBulkTagging;
//
//        @ApiModelProperty(value = "Link to where to PUT transactions where new tags will be set", required = true)
//        private LinkDTO similarTransactionsForBulkTaggingSetTags;
//
//        @ApiModelProperty(value = "Link to get the payment cycle data")
//        private LinkDTO paymentCycle;
//    }
}
