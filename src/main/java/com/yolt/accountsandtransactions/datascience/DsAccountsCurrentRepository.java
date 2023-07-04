package com.yolt.accountsandtransactions.datascience;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.mapping.Mapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.cassandra.CassandraRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.*;

/**
 * Do **NOT** use the {@link Session} that is available on the {@link ApplicationContext}.
 */
@Repository
@Slf4j
public class DsAccountsCurrentRepository extends CassandraRepository<DsAccountCurrent> {

    @Autowired
    public DsAccountsCurrentRepository(final DataScienceCassandraSession ds) {
        super(ds.getSession(), DsAccountCurrent.class);
    }

    @Override
    public void save(final DsAccountCurrent accountCurrent) {
        Update updateQuery = createUpdate();

        // We always should reset current and available balance (even if they are null)
        updateQuery.with(set(DsAccountCurrent.CURRENT_BALANCE_COLUMN, accountCurrent.getCurrentBalance()))
                .and(set(DsAccountCurrent.AVAILABLE_BALANCE_COLUMN, accountCurrent.getAvailableBalance()))
                .and(set(DsAccountCurrent.EXTENDED_ACCOUNT_COLUMN, accountCurrent.getExtendedAccount()));

        // For other values and for regression purposes we set values only if they are not null
        if (accountCurrent.getUserSiteId() != null) {
            updateQuery.with(set(DsAccountCurrent.USER_SITE_ID_COLUMN, accountCurrent.getUserSiteId()));
        }
        if (accountCurrent.getSiteId() != null) {
            updateQuery.with(set(DsAccountCurrent.SITE_ID_COLUMN, accountCurrent.getSiteId()));
        }
        if (accountCurrent.getExternalAccountId() != null) {
            updateQuery.with(set(DsAccountCurrent.EXTERNAL_ACCOUNT_ID_COLUMN, accountCurrent.getExternalAccountId()));
        }
        if (accountCurrent.getExternalSiteId() != null) {
            updateQuery.with(set(DsAccountCurrent.EXTERNAL_SITE_ID_COLUMN, accountCurrent.getExternalSiteId()));
        }
        if (accountCurrent.getName() != null) {
            updateQuery.with(set(DsAccountCurrent.NAME_COLUMN, accountCurrent.getName()));
        }
        if (accountCurrent.getAccountType() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_TYPE_COLUMN, accountCurrent.getAccountType()));
        }
        if (accountCurrent.getCurrencyCode() != null) {
            updateQuery.with(set(DsAccountCurrent.CURRENCY_CODE_COLUMN, accountCurrent.getCurrencyCode()));
        }
        if (accountCurrent.getLastUpdatedTime() != null) {
            updateQuery.with(set(DsAccountCurrent.LAST_UPDATED_TIME_COLUMN, accountCurrent.getLastUpdatedTime()));
        }
        if (accountCurrent.getStatus() != null) {
            updateQuery.with(set(DsAccountCurrent.STATUS_COLUMN, accountCurrent.getStatus()));
        }
        if (accountCurrent.getStatusDetail() != null) {
            updateQuery.with(set(DsAccountCurrent.STATUS_DETAIL_COLUMN, accountCurrent.getStatusDetail()));
        }
        if (accountCurrent.getProvider() != null) {
            updateQuery.with(set(DsAccountCurrent.PROVIDER_COLUMN, accountCurrent.getProvider()));
        }
        if (accountCurrent.getHidden() != null) {
            updateQuery.with(set(DsAccountCurrent.HIDDEN_COLUMN, accountCurrent.getHidden()));
        }
        if (accountCurrent.getLinkedAccount() != null) {
            updateQuery.with(set(DsAccountCurrent.LINKED_ACCOUNT_COLUMN, accountCurrent.getLinkedAccount()));
        }
        if (accountCurrent.getBankSpecific() != null) {
            updateQuery.with(set(DsAccountCurrent.BANK_SPECIFIC_COLUMN, accountCurrent.getBankSpecific()));
        }
        if (accountCurrent.getAccountHolderName() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_HOLDER_NAME, accountCurrent.getAccountHolderName()));
        }
        if (accountCurrent.getAccountScheme() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_SCHEME, accountCurrent.getAccountScheme()));
        }
        if (accountCurrent.getAccountIdentification() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_IDENTIFICATION, accountCurrent.getAccountIdentification()));
        }
        if (accountCurrent.getAccountSecondaryIdentification() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_SECONDARY_IDENTIFICATION, accountCurrent.getAccountSecondaryIdentification()));
        }
        if (accountCurrent.getAccountMaskedIdentification() != null) {
            updateQuery.with(set(DsAccountCurrent.ACCOUNT_MASKED_IDENTIFICATION, accountCurrent.getAccountMaskedIdentification()));
        }
        updateQuery.with(set(DsAccountCurrent.CLOSED, accountCurrent.isClosed()));

        updateQuery.where(eq(DsAccountCurrent.USER_ID_COLUMN, accountCurrent.getUserId()))
                .and(eq(DsAccountCurrent.ACCOUNT_ID_COLUMN, accountCurrent.getAccountId()));

        if (updateQuery.getConsistencyLevel() == null) {
            updateQuery.setConsistencyLevel(writeConsistency);
        }
        this.session.execute(updateQuery);

        log.debug("Saved {}", accountCurrent);
    }

    public List<DsAccountCurrent> getAccountsForUser(final UUID userId) {
        Select select = super.createSelect();
        select.where(eq(DsAccountCurrent.USER_ID_COLUMN, userId));
        return select(select);
    }

    public Mapper<DsAccountCurrent> getMapper() {
        return mapper;
    }

    public void updateUserSiteId(@NonNull UUID userId, @NonNull UUID accountId, @NonNull UUID newUserSiteId) {
        var query = update("account_current")
                .with(set("user_site_id", newUserSiteId))
                .where(eq("user_id", userId))
                .and(eq("account_id", accountId));
        session.execute(query);
    }

    /**
     * Unset (nullify) the bank_specific field in the accounts_current table
     *
     * @param userId    the user-id
     * @param accountId the account-id
     */
    public void unsetBankSpecific(@NonNull UUID userId, @NonNull UUID accountId) {

        var query = update("account_current")
                .with(set("bank_specific", null))
                .where(eq("user_id", userId))
                .and(eq("account_id", accountId));
        session.execute(query);
    }

    public void deleteAccount(DsAccountCurrent account) {
        mapper.delete(account);
    }
}
