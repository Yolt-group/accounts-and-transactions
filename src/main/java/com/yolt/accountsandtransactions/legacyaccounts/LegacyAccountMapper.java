package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.accounts.Account;
import com.yolt.accountsandtransactions.accounts.Balance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.extendeddata.account.BalanceDTO;
import nl.ing.lovebird.extendeddata.account.ExtendedAccountDTO;
import nl.ing.lovebird.extendeddata.account.Status;
import nl.ing.lovebird.extendeddata.common.AccountReferenceDTO;
import nl.ing.lovebird.extendeddata.common.BalanceAmountDTO;
import nl.ing.lovebird.extendeddata.common.CurrencyCode;
import nl.ing.lovebird.extendeddata.transaction.AccountReferenceType;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

@Slf4j
@Component
@RequiredArgsConstructor
public class LegacyAccountMapper {

    /**
     * Maps an Account to an LegacyAccountDTO.
     *
     * @param account the account to map
     * @return the corresponding LegacyAccountDTO
     */
    public LegacyAccountDTO fromAccount(final Account account) {

        final var lastRefreshedDateTime = account.getLastDataFetchTime() != null ?
                Date.from(account.getLastDataFetchTime()) :
                null;

        return LegacyAccountDTO.builder()
                .id(account.getId())
                .externalId(account.getExternalId())
                .name(account.getName())
                .currencyCode(toCurrency(account.getCurrency()))
                .balance(account.getBalance())
                .availableBalance(account.getAvailableBalance())
                .lastRefreshed(lastRefreshedDateTime)
                .updated(lastRefreshedDateTime)
                .status(account.getStatus() == Account.Status.ENABLED ? LegacyAccountStatusCode.DATASCIENCE_FINISHED : LegacyAccountStatusCode.DELETED)
                .userSite(new LegacyUserSiteDTO(account.getUserSiteId(), account.getSiteId()))
                .type(LegacyAccountType.from(account.getType()))
                .hidden(account.isHidden())
                .closed(BooleanUtils.isTrue(account.getStatus() == Account.Status.DELETED))
                .migrated(true)
                .accountNumber(LegacyAccountNumberDTO.from(account))
                .accountMaskedIdentification(account.getMaskedPan())
                .linkedAccount(account.getLinkedAccount())
                .bankSpecific(account.getBankSpecific())
                .extendedAccount(getExtendedAccount(account))
                .links(createAccountLinks(account.getId()))
                .build();
    }

    private ExtendedAccountDTO getExtendedAccount(final Account account) {
        List<AccountReferenceDTO> accountReferences = new ArrayList<>();
        if (isNotBlank(account.getIban())) {
            accountReferences.add(AccountReferenceDTO.builder().type(AccountReferenceType.IBAN).value(account.getIban()).build());
        }
        if (isNotBlank(account.getBban())) {
            accountReferences.add(AccountReferenceDTO.builder().type(AccountReferenceType.BBAN).value(account.getBban()).build());
        }
        if (isNotBlank(account.getMaskedPan())) {
            accountReferences.add(AccountReferenceDTO.builder().type(AccountReferenceType.MASKED_PAN).value(account.getMaskedPan()).build());
        }
        if (isNotBlank(account.getPan())) {
            accountReferences.add(AccountReferenceDTO.builder().type(AccountReferenceType.PAN).value(account.getPan()).build());
        }
        if (isNotBlank(account.getSortCodeAccountNumber())) {
            accountReferences.add(AccountReferenceDTO.builder().type(AccountReferenceType.SORTCODEACCOUNTNUMBER).value(account.getSortCodeAccountNumber()).build());
        }

        return ExtendedAccountDTO.builder()
                .accountReferences(accountReferences)
                .currency(account.getCurrency())
                .name(account.getName())
                .product(account.getProduct())
                .status(mapStatus(account.getStatus()))
                .bic(account.getBic())
                .linkedAccounts(account.getLinkedAccount())
                .usage(account.getUsage())
                .balances(toBalanceDTOList(account.getBalances()))
                .build();
    }

    private LegacyAccountDTO.LinksDTO createAccountLinks(final UUID accountId) {
        return new LegacyAccountDTO.LinksDTO(
                new LinkDTO("/accounts/user-accounts/me/accounts/hide-unhide"),
                createTransactionsByAccountLink(accountId));
    }

    /**
     * Returns the next link as if transactions pod would return it, i.e. /transactions/transactions-by-account/me.
     * client-proxy maps this link to /legacy-transactions/transactions-by-account/me
     *
     * @param accountId account Id
     * @return link to transactions by account
     */
    static LinkDTO createTransactionsByAccountLink(final UUID accountId) {

        MultiValueMap<String, String> queryParams = new LinkedMultiValueMap<>();
        queryParams.add("accountId", accountId.toString());

        UriComponents uriComponents = UriComponentsBuilder.fromPath("/transactions/transactions-by-account/me")
                .queryParams(queryParams)
                .encode()
                .build();
        return new LinkDTO(uriComponents.toString());
    }

    private static List<BalanceDTO> toBalanceDTOList(final List<Balance> balances) {
        return balances != null ?
                balances.stream().map(LegacyAccountMapper::toBalanceDTO).collect(Collectors.toList()) :
                List.of();
    }

    private static BalanceDTO toBalanceDTO(final Balance balance) {
        return BalanceDTO.builder()
                .balanceAmount(BalanceAmountDTO.builder()
                        .amount(balance.getAmount())
                        .currency(balance.getCurrency())
                        .build())
                .balanceType(balance.getBalanceType())
                .lastChangeDateTime(Optional.ofNullable(balance.getLastChangeDateTime())
                        .map(instant -> instant.atZone(ZoneOffset.UTC))
                        .orElse(null))
                .build();
    }

    private static nl.ing.lovebird.extendeddata.account.Status mapStatus(Account.Status accountStatus) {
        return switch (accountStatus) {
            case ENABLED -> Status.ENABLED;
            case DELETED -> Status.DELETED;
            case BLOCKED -> Status.BLOCKED;
        };
    }

    private static Currency toCurrency(final CurrencyCode currencyCode) {
        return Currency.getInstance(currencyCode.name());
    }
}
