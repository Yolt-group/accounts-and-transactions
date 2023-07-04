package com.yolt.accountsandtransactions.accounts;

import com.yolt.accountsandtransactions.accounts.AccountDTO.BalanceDTO;
import com.yolt.accountsandtransactions.accounts.event.AccountEvent;
import com.yolt.accountsandtransactions.accounts.event.AccountEventType;
import com.yolt.accountsandtransactions.datascience.DsAccountDataDeletionService;
import com.yolt.accountsandtransactions.offloading.OffloadService;
import com.yolt.accountsandtransactions.transactions.Transaction;
import com.yolt.accountsandtransactions.transactions.TransactionService;
import com.yolt.accountsandtransactions.transactions.TransactionsPageDTO;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientToken;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.account.Status;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import nl.ing.lovebird.providerdomain.AccountType;
import nl.ing.lovebird.providerdomain.ProviderAccountDTO;
import nl.ing.lovebird.providerdomain.ProviderAccountNumberDTO;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.time.chrono.ChronoZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNumeric;
import static org.apache.commons.lang3.StringUtils.strip;

@RequiredArgsConstructor
@Service
@Slf4j
public class AccountService {

    private final Clock clock;
    private final AccountRepository accountRepository;
    private final AccountEventsProducer accountEventsProducer;
    private final OffloadService offloadService;
    private final TransactionService transactionService;
    private final DsAccountDataDeletionService dsAccountDataDeletionService;

    // TODO move this out of the AccountService to controller layer
    public List<AccountDTO> getAccountsDTOsForUserSite(@NonNull UUID userId, @Nullable UUID userSiteId) {
        Stream<Account> stream = accountRepository.getAccounts(userId).stream();
        if (userSiteId != null) {
            stream = stream.filter(it -> it.getUserSiteId().equals(userSiteId));
        }
        return stream.map(this::map).collect(toList());
    }

    public Map<UUID, UUID> getUserSiteIdsForAccountIds(@NonNull UUID userId, @NonNull Collection<UUID> accountIds) {
        return accountRepository.getUserSiteIdsForAccountIds(userId, accountIds);
    }

    public List<Account> getAccountsForUserSite(final @NonNull ClientUserToken clientUserToken, final @NonNull UUID userSiteId) {
        return accountRepository.getAccounts(clientUserToken.getUserIdClaim()).stream()
                .filter(account -> account.getUserSiteId().equals(userSiteId))
                .collect(Collectors.toList());
    }

    public Account createOrUpdateAccount(final ClientUserToken clientUserToken,
                                         final ProviderAccountDTO providerAccountDTO,
                                         final UUID accountId,
                                         final UUID userSiteId,
                                         final UUID siteId,
                                         boolean isPresent,
                                         final Instant lastDataFetchedTime) {
        Account account = map(clientUserToken.getUserIdClaim(), providerAccountDTO, accountId, userSiteId, siteId, lastDataFetchedTime, isPresent);
        accountRepository.upsert(account);
        AccountEventType type = isPresent ? AccountEventType.UPDATED : AccountEventType.CREATED;
        produceKafkaEvent(clientUserToken, account, type);

        offloadService.offloadInsertOrUpdateAsync(account, clientUserToken.getClientIdClaim());

        return account;
    }

    // TODO move this out of the AccountService to controller layer
    public AccountDTO map(Account account) {

        AccountDTO.AccountDTOBuilder accountDTOBuilder = AccountDTO.builder()
                .id(account.getId())
                .externalId(account.getExternalId())
                .type(account.getType())
                .createdAt(account.getCreatedAtOrDefault())
                .userSite(new AccountDTO.UserSiteDTO(
                        account.getUserSiteId(),
                        account.getSiteId()
                ))
                .currency(account.getCurrency())
                .balance(account.getBalance())
                .status(account.getStatus())
                .name(account.getName())
                .product(account.getProduct())
                .accountHolder(account.getAccountHolder())
                .bankSpecific(account.getBankSpecific())
                .linkedAccount(account.getLinkedAccount())
                .lastDataFetchTime(account.getLastDataFetchTime())
                .usage(account.getUsage());

        if (account.getType() == AccountType.CREDIT_CARD) {
            accountDTOBuilder.creditCardAccount(new AccountDTO.CreditCardAccountDTO(account.getCreditLimit(), account.getAvailableCredit(), account.getLinkedAccount()));
        }
        if (account.getType() == AccountType.CURRENT_ACCOUNT) {
            accountDTOBuilder.currentAccount(new AccountDTO.CurrentAccountDTO(account.getBic(), account.getCreditLimit()));
        }
        if (account.getType() == AccountType.SAVINGS_ACCOUNT) {
            accountDTOBuilder.savingsAccount(new AccountDTO.SavingsAccountDTO(account.getBic(), account.getIsMoneyPotOf()));
        }
        if (isNotBlank(account.getIban()) || isNotBlank(account.getBban()) || isNotBlank(account.getMaskedPan())
                || isNotBlank(account.getPan()) || isNotBlank(account.getSortCodeAccountNumber())) {
            accountDTOBuilder.accountReferences(new AccountReferencesDTO(account.getIban(), account.getMaskedPan(), account.getPan(), account.getBban(), account.getSortCodeAccountNumber()));
        }

        var balanceDTOS = Optional.ofNullable(account.getBalances())
                .map(this::mapToExternalBalanceDTOs)
                .orElse(emptyList());

        accountDTOBuilder.balances(balanceDTOS);

        return accountDTOBuilder.build();
    }

    private void produceKafkaEvent(ClientToken clientToken, Account account, AccountEventType type) {
        AccountEvent event = new AccountEvent(type, account.getUserId(), account.getUserSiteId(), account.getId(), account.getSiteId(), account.getAccountHolder());
        accountEventsProducer.sendMessage(event, clientToken);
    }

    public Account map(final UUID userId,
                       final ProviderAccountDTO providerAccountDTO,
                       final UUID accountId,
                       final UUID userSiteId,
                       final UUID siteId,
                       final Instant lastDataFetchTime, boolean isPresent) {
        ExtendedAccountDTO extendedAccount = providerAccountDTO.getExtendedAccount();

        var balances = Optional.ofNullable(providerAccountDTO.getExtendedAccount())
                .map(ExtendedAccountDTO::getBalances)
                .map(this::mapToInternalBalances)
                .orElse(emptyList());

        Account.AccountBuilder builder = Account.builder()
                .userId(userId)
                .id(accountId)
                .externalId(providerAccountDTO.getAccountId())
                .type(providerAccountDTO.getYoltAccountType())
                .currency(providerAccountDTO.getCurrency())
                .balance(providerAccountDTO.getCurrentBalance() != null ? providerAccountDTO.getCurrentBalance() : providerAccountDTO.getAvailableBalance())
                .status(toStatus(providerAccountDTO))
                .name(providerAccountDTO.getName())
                .iban(getIbanOrNull(providerAccountDTO))
                .sortCodeAccountNumber(getSortCodeAccountNumberOrNull(providerAccountDTO))
                .bic(providerAccountDTO.getBic())
                .linkedAccount(providerAccountDTO.getLinkedAccount())
                .bankSpecific(providerAccountDTO.getBankSpecific())
                .lastDataFetchTime(lastDataFetchTime)
                .userSiteId(userSiteId)
                .balances(balances)
                .siteId(siteId);

        if (extendedAccount != null) {
            builder.product(extendedAccount.getProduct())
                    .maskedPan(getAccountReferenceFromExtendedModel(extendedAccount, AccountReferenceType.MASKED_PAN))
                    .pan(getAccountReferenceFromExtendedModel(extendedAccount, AccountReferenceType.PAN))
                    .bban(getAccountReferenceFromExtendedModel(extendedAccount, AccountReferenceType.BBAN))
                    .linkedAccount(extendedAccount.getLinkedAccounts())
                    .usage(extendedAccount.getUsage());
        }

        if (providerAccountDTO.getAccountNumber() != null) {
            builder.accountHolder(providerAccountDTO.getAccountNumber().getHolderName());
        }

        if (providerAccountDTO.getCreditCardData() != null) {
            builder.creditLimit(providerAccountDTO.getCreditCardData().getTotalCreditLineAmount())
                    .availableCredit(providerAccountDTO.getCreditCardData().getAvailableCreditAmount());
        }

        // set the createdAt only if this is a new account;
        if (!isPresent) {
            builder.createdAt(Instant.now(clock));
        }

        // TODO: interest rate. get from provider model once they provide it
        // TODO creditlimit should also be provided when we're dealing with current accounts. But from a different field compared to credit cards.
        // TODO ismoneypotof :')  get from provider model once they provide it.
        return builder.build();
    }

    public static String getAccountReferenceFromExtendedModel(ExtendedAccountDTO extendedAccount, AccountReferenceType referenceType) {
        final String accountNumber = Optional.ofNullable(extendedAccount)
                .map(ExtendedAccountDTO::getAccountReferences)
                .orElse(emptyList())
                .stream()
                .filter(t -> t.getType() == referenceType)
                .findFirst()
                .map(AccountReferenceDTO::getValue)
                .orElse(null);

        logWarningForUnmaskedPan(accountNumber, referenceType);
        return accountNumber;
    }

    private static void logWarningForUnmaskedPan(String accountNumber, AccountReferenceType referenceType) {
        if (accountNumber != null &&
                (referenceType.equals(AccountReferenceType.PAN) || referenceType.equals(AccountReferenceType.MASKED_PAN))) {
            String strippedAccountNumber = strip(accountNumber, " ");
            if ((strippedAccountNumber.length() == 15 || strippedAccountNumber.length() == 16) && isNumeric(accountNumber)) {
                log.warn("Potentially received full PAN from bank");
            }
        }
    }

    public static String getIbanOrNull(ProviderAccountDTO providerAccountDTO) {
        if (providerAccountDTO.getAccountNumber() != null &&
                providerAccountDTO.getAccountNumber().getScheme() == ProviderAccountNumberDTO.Scheme.IBAN) {
            return providerAccountDTO.getAccountNumber().getIdentification();
        } else {
            // fallback
            return getAccountReferenceFromExtendedModel(providerAccountDTO.getExtendedAccount(), AccountReferenceType.IBAN);
        }
    }

    public static String getSortCodeAccountNumberOrNull(ProviderAccountDTO providerAccountDTO) {
        if (providerAccountDTO.getAccountNumber() != null &&
                providerAccountDTO.getAccountNumber().getScheme() == ProviderAccountNumberDTO.Scheme.SORTCODEACCOUNTNUMBER) {
            return providerAccountDTO.getAccountNumber().getIdentification();
        } else {
            // fallback
            return getAccountReferenceFromExtendedModel(providerAccountDTO.getExtendedAccount(), AccountReferenceType.SORTCODEACCOUNTNUMBER);
        }
    }

    Account.Status toStatus(ProviderAccountDTO providerAccountDTO) {
        if (Boolean.TRUE.equals(providerAccountDTO.getClosed())) {
            return Account.Status.DELETED;
        }
        if (providerAccountDTO.getExtendedAccount() != null && providerAccountDTO.getExtendedAccount().getStatus() == Status.BLOCKED) {
            return Account.Status.BLOCKED;
        }
        return Account.Status.ENABLED;
    }

    /**
     * Delete all {@link Account}s and {@link Transaction}s for the given <code>user-id</code> and <code>userSiteId</code>
     *
     * @param userId     the user-id for which to delete the accounts and transactions
     * @param userSiteId the user-site id for which to delete the accounts and transactions
     */
    public void deleteAccountsAndTransactionsForUserSite(@NonNull UUID userId, @NonNull UUID userSiteId) {
        log.info("Deleting all accounts and transactions for user-site {}", userSiteId);

        var accounts = accountRepository.getAccounts(userId).stream()
                .filter(it -> userSiteId.equals(it.getUserSiteId()))
                .collect(toMap(Account::getId, identity()));

        deleteAccountsAndTransactionsForAccounts(userId, accounts);

        dsAccountDataDeletionService.deleteAccountData(userId, List.copyOf(accounts.keySet()));
    }

    /**
     * Delete all accounts and transactions for the given <code>user-id</code> and a set of <code>accounts</code>
     * with elements account-id -> account
     *
     * @param userId   the user-id for which to delete the accounts and transactions
     * @param accounts the list of account-id -> account which to delete
     */
    public void deleteAccountsAndTransactionsForAccounts(@NonNull UUID userId, @NonNull Map<UUID, Account> accounts) {
        log.info("Deleting all accounts and transactions for accounts [{}]", accounts.keySet());

        // offload the account delete
        accounts.forEach((uuid, account) -> offloadService.offloadDeleteAsync(account));

        var accountIds = List.copyOf(accounts.keySet());
        transactionService.deleteAllTransactionDataForUserAccounts(userId, accountIds);
        accountRepository.deleteAccounts(userId, accountIds);
    }

    /**
     * Delete all accounts for the given <code>userId</code>
     *
     * @param userId the user-id for which to delete the accounts
     */
    public void deleteAccountsAndTransactionsForUser(@NonNull UUID userId) {
        log.info("Deleting all accounts and transactions for user {}", userId);

        // collect all accounts for the given user
        var accounts = accountRepository.getAccounts(userId).stream()
                .collect(toMap(Account::getId, identity()));

        deleteAccountsAndTransactionsForAccounts(userId, accounts);
    }

    public TransactionsPageDTO getTransactionsForAccount(UUID userId, List<UUID> accountIds, DateInterval dateInterval, String next, int pageSize) {
        final var userAccountsIds = getAccountsDTOsForUserSite(userId, null).stream()
                .map(AccountDTO::getId)
                .collect(toList());

        if (accountIds == null || accountIds.isEmpty()) {
            accountIds = userAccountsIds;
        } else {
            // Temporary code to see how often customers request transactions for account ids that don't exist. See YCO-2050.
            logInvalidAccountIdsForUser(userAccountsIds, accountIds);
        }

        return transactionService.getTransactions(userId, accountIds, dateInterval, next, pageSize);
    }

    private List<BalanceDTO> mapToExternalBalanceDTOs(List<Balance> balances) {
        return balances.stream()
                .map(it -> BalanceDTO.builder()
                        .amount(it.getAmount())
                        .currency(it.getCurrency().name())
                        .type(it.getBalanceType())
                        .build())
                .collect(toList());
    }

    /**
     * When mapping these balances we are heavily dependent on what we get from providers and the banks.
     * If a client has issues with their expected balances not showing up first check if the balance was formed
     * correctly in the providers pod/ or RDD.
     */
    private List<Balance> mapToInternalBalances(List<nl.ing.lovebird.extendeddata.account.BalanceDTO> balances) {
        return balances.stream()
                .filter(this::validateAmount)
                .map(it -> Balance.builder()
                        .amount(it.getBalanceAmount().getAmount())
                        .currency(it.getBalanceAmount().getCurrency())
                        .balanceType(it.getBalanceType())
                        .lastChangeDateTime(Optional.ofNullable(it.getLastChangeDateTime()).map(ChronoZonedDateTime::toInstant).orElse(null))
                        .build())
                .collect(toList());
    }

    private boolean validateAmount(nl.ing.lovebird.extendeddata.account.BalanceDTO balance) {
        try {
            var balanceAmount = balance.getBalanceAmount();
            balanceAmount.validate();
        } catch (NullPointerException npe) {
            log.warn("Invalid balance amount found, skipping.", npe);
            return false;
        }
        return true;
    }

    private void logInvalidAccountIdsForUser(List<UUID> userAccountIds, List<UUID> requestedAccountIds) {
        final var nonExistingAccountIds = requestedAccountIds.stream()
                .filter(requestedAccountId -> !userAccountIds.contains(requestedAccountId))
                .collect(Collectors.toSet());

        if (!nonExistingAccountIds.isEmpty()) {
            log.warn("Received transactions request for the following invalid account ids: {}", nonExistingAccountIds);
        }
    }
}
