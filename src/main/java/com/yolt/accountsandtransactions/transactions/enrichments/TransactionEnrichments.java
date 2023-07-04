package com.yolt.accountsandtransactions.transactions.enrichments;

import com.datastax.driver.mapping.annotations.ClusteringColumn;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.Transient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import nl.ing.lovebird.cassandra.codec.LocalDateTypeCodec;
import org.springframework.lang.Nullable;

import javax.validation.constraints.NotNull;
import java.time.LocalDate;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static java.util.Collections.emptySet;

@AllArgsConstructor
@NoArgsConstructor
@Table(name = TransactionEnrichments.TRANSACTION_ENRICHMENTS_TABLE)
@Data
@Builder
public class TransactionEnrichments {
    public static final String TRANSACTION_ENRICHMENTS_TABLE = "transaction_enrichments";
    public static final String USER_ID_COLUMN = "user_id";
    public static final String ACCOUNT_ID_COLUMN = "account_id";
    public static final String DATE_COLUMN = "date";
    public static final String ID_COLUMN = "id";
    /**
     * This column should actually be called enrichment_category_personal but renaming this column would have us write
     * a batch to work around the cassandra column renaming limitations (i.e. we cannot rename a non-primary column...)
     * For more context, refer to YCO-1685.
     */
    public static final String ENRICHMENT_CATEGORY_COLUMN = "enrichment_category";
    public static final String ENRICHMENT_CATEGORY_SME_COLUMN = "enrichment_category_sme";
    public static final String ENRICHMENT_MERCHANT_NAME_COLUMN = "enrichment_merchant_name";
    public static final String ENRICHMENT_COUNTERPARTY_NAME_COLUMN = "enrichment_counterparty_name";
    public static final String ENRICHMENT_COUNTERPARTY_KNOWN_MERCHANT_COLUMN = "enrichment_counterparty_known_merchant";
    public static final String ENRICHMENT_CYCLE_ID_COLUMN = "enrichment_cycle_id";
    public static final String ENRICHMENT_LABELS_COLUMN = "enrichment_labels";

    @NotNull
    @PartitionKey
    @Column(name = USER_ID_COLUMN)
    private UUID userId;
    @NotNull
    @ClusteringColumn(0)
    @Column(name = ACCOUNT_ID_COLUMN)
    private UUID accountId;
    @NotNull
    @ClusteringColumn(1)
    @Column(name = DATE_COLUMN, codec = LocalDateTypeCodec.class)
    private LocalDate date;
    @NotNull
    @ClusteringColumn(2)
    @Column(name = ID_COLUMN)
    private String id;

    @Column(name = ENRICHMENT_CATEGORY_COLUMN)
    private String enrichmentCategoryPersonal;
    @Column(name = ENRICHMENT_CATEGORY_SME_COLUMN)
    private String enrichmentCategorySME;

    /*
     * Use enrichmentCounterpartyName and enrichmentCounterpartyIsKnownMerchant
     */
    @Deprecated
    @Column(name = ENRICHMENT_MERCHANT_NAME_COLUMN)
    private String enrichmentMerchantName;

    @Column(name = ENRICHMENT_COUNTERPARTY_NAME_COLUMN)
    private String enrichmentCounterpartyName;
    @Column(name = ENRICHMENT_COUNTERPARTY_KNOWN_MERCHANT_COLUMN)
    private Boolean enrichmentCounterpartyIsKnownMerchant; // do not change to primitive boolean, this does not work well with Cassandra

    @Column(name = ENRICHMENT_CYCLE_ID_COLUMN)
    private UUID enrichmentCycleId;

    @Column(name = ENRICHMENT_LABELS_COLUMN)
    private Set<String> enrichmentLabels;

    /**
     * The enrichmentMerchantName is not longer filled; instead, enrichmentCounterpartyName and enrichmentCounterpartyIsKnownMerchant
     * are filled (as of sept 1 2021)
     * <p/>
     * Since the replacement fields have been added at a later time, they do not contain merchant/ counterparty data for all the transactions.
     * <p/>
     * For transactions for which we do not have the counterparty columns filled, we fall back on the enrichmentMerchantName (if any)
     * <p>
     * This fields is deprecated but exists for (client) compatibility reasons.
     */
    @Transient
    @Deprecated
    public Optional<String> getMerchantName() {
        if (enrichmentCounterpartyName != null && enrichmentCounterpartyIsKnownMerchant != null) {
            return Optional.of(enrichmentCounterpartyName)
                    .filter(ignored -> enrichmentCounterpartyIsKnownMerchant);
        } else {
            return Optional.ofNullable(enrichmentMerchantName);
        }
    }

    @Transient
    public Optional<Counterparty> getCounterparty() {
        return Optional.ofNullable(enrichmentCounterpartyName)
                .map(name -> new Counterparty(name, primitive(enrichmentCounterpartyIsKnownMerchant)));
    }

    @Transient
    public Optional<UUID> getCycle() {
        return Optional.ofNullable(enrichmentCycleId);
    }

    @Transient
    public Set<String> getLabelsOrEmptySet() {
        return Optional.ofNullable(enrichmentLabels).orElse(emptySet());
    }

    public static boolean primitive(final @Nullable Boolean bool) {
        return bool != null && bool;
    }

    @ToString
    @EqualsAndHashCode
    @RequiredArgsConstructor
    public static class Counterparty {

        @NonNull
        public final String name;

        public final boolean isKnownMerchant;
    }

}
