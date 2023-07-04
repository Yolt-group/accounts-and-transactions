package com.yolt.accountsandtransactions.inputprocessing.enrichments.api.categories;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransaction;
import com.yolt.accountsandtransactions.inputprocessing.enrichments.api.base.EnrichedTransactionKey;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Value;

import java.util.Map;
import java.util.Optional;

@Data
public class CategoriesEnrichedTransaction extends EnrichedTransaction {

    /**
     * Use {@link #categories} instead.
     */
    @NonNull
    @Deprecated
    private final String category;

    /**
     * Use {@link #categories} instead.
     */
    @NonNull
    @Deprecated
    private final String source;

    /**
     * The categories element contains a map of category-type (sme, personal) -> category + source.
     */
    @NonNull
    private final Map<CategoryEnrichmentType, CategoryDetected> categories;

    @Builder
    @JsonCreator
    public CategoriesEnrichedTransaction(
            @NonNull @JsonProperty("key") final EnrichedTransactionKey key,
            @NonNull @JsonProperty("categories") final Map<CategoryEnrichmentType, CategoryDetected> categories,
            @NonNull @JsonProperty("category") final String category,
            @NonNull @JsonProperty("source") final String source) {
        super(key);
        this.categories = categories;
        this.category = category;
        this.source = source;
    }

    public Optional<String> getSMECategory() {
        return getCategory(CategoryEnrichmentType.sme_categories);
    }

    public Optional<String> getPersonalCategory() {
        return getCategory(CategoryEnrichmentType.personal_categories);
    }

    private Optional<String> getCategory(final CategoryEnrichmentType enrichmentType) {
        return Optional.ofNullable(this.categories.get(enrichmentType))
                .map(categoryDetected -> categoryDetected.category);
    }

    @Override
    public String toString() {
        return "CategoriesEnrichedTransaction";
    }

    @Value
    public static class CategoryDetected {

        @NonNull String category;

        /**
         * The source of the category. Can be one of:
         * <ul>
         *     <li>UserSingleFeedback</li>
         *     <li>UserMultiFeedback</li>
         *     <li>ModelPrediction</li>
         *     <li>ModelFallback</li>
         *     <li>Unknown</li>
         * </ul>
         * <p>
         * This value is not used and only here for completeness
         */
        @NonNull String categorySource;
    }

    enum CategoryEnrichmentType {
        sme_categories,
        personal_categories
    }
}
