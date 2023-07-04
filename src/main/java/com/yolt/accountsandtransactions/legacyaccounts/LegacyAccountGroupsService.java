package com.yolt.accountsandtransactions.legacyaccounts;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.logging.LogTypeMarker;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Currency;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class LegacyAccountGroupsService {

    public List<LegacyAccountGroupDTO> buildAccountGroups(final List<LegacyAccountDTO> accounts) {
        Currency preferredCurrencyCode = Currency.getInstance("EUR");

        log.debug("Building account groups using preferred currency {}", preferredCurrencyCode);
        List<LegacyAccountGroupDTO> accountGroups = new ArrayList<>();

        for (LegacyAccountDTO account : accounts) {
            log.debug("Processing account {}", account);
            addAccountToGroup(accountGroups, account, preferredCurrencyCode);
        }

        accountGroups.forEach(LegacyAccountGroupsService::calculateTotalsForGroup);
        accountGroups.sort(Comparator.comparingInt(a -> a.getType().ordinal()));

        return accountGroups;
    }

    private static void addAccountToGroup(final List<LegacyAccountGroupDTO> accountGroups, final LegacyAccountDTO account, final Currency preferredCurrency) {
        LegacyAccountType accountGroupName = account.getType() != null ?
                account.getType() :
                LegacyAccountType.UNKNOWN;

        // Determine whether foreign currency account
        if (!LegacyAccountType.UNKNOWN.equals(accountGroupName)) {
            if (account.getCurrencyCode() == null) {
                log.warn(LogTypeMarker.getDataErrorMarker(), "No currency for account {} with name '{}'", account.getId(), account.getName()); // NOSHERIFF
                accountGroupName = LegacyAccountType.FOREIGN_CURRENCY;
            } else if (!account.getCurrencyCode().equals(preferredCurrency)) {
                accountGroupName = LegacyAccountType.FOREIGN_CURRENCY;
            }
        }

        // Find the group
        LegacyAccountGroupDTO accountGroup = null;
        for (LegacyAccountGroupDTO ag : accountGroups) {
            if (ag.getType().equals(accountGroupName)) {
                accountGroup = ag;
                break;
            }
        }
        // Create if not exist
        if (accountGroup == null) {
            accountGroup = new LegacyAccountGroupDTO(accountGroupName);
            accountGroups.add(accountGroup);
        }

        accountGroup.getAccounts().add(account);
    }

    private static void calculateTotalsForGroup(final LegacyAccountGroupDTO accountGroup) {
        Map<Currency, BigDecimal> totalsPerCurrency = new HashMap<>();
        Set<Currency> currencyCodesWithoutBalance = new HashSet<>();

        accountGroup.getAccounts().stream()
                .filter(account -> !isAccountInvisible(account))
                .filter(account -> account.getCurrencyCode() != null)
                .forEach(account -> {
                    if (account.getBalance() == null) {
                        currencyCodesWithoutBalance.add(account.getCurrencyCode());
                        return;
                    }
                    BigDecimal balance = account.getBalance();
                    BigDecimal totalForCurrency = totalsPerCurrency.get(account.getCurrencyCode());
                    if (totalForCurrency == null) {
                        totalsPerCurrency.put(account.getCurrencyCode(), balance);
                    } else {
                        totalsPerCurrency.replace(account.getCurrencyCode(), totalForCurrency.add(balance));
                    }
                });

        List<LegacyAccountGroupDTO.TotalDTO> totals = totalsPerCurrency.entrySet()
                .stream()
                .map(totalPerCurrency -> new LegacyAccountGroupDTO.TotalDTO(totalPerCurrency.getKey(), totalPerCurrency.getValue()))
                .filter(t -> !currencyCodesWithoutBalance.contains(t.getCurrencyCode()))
                .collect(Collectors.toList());

        accountGroup.setTotals(totals);
    }

    private static boolean isAccountInvisible(final LegacyAccountDTO account) {
        return LegacyAccountStatusCode.DELETED.equals(account.getStatus()) || account.isHidden();
    }
}