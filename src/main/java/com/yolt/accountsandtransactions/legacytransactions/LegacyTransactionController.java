package com.yolt.accountsandtransactions.legacytransactions;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import nl.ing.lovebird.clienttokens.ClientUserToken;
import nl.ing.lovebird.clienttokens.annotations.VerifiedClientToken;
import com.yolt.accountsandtransactions.datetime.DateInterval;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

/**
 * Reimplementation of the endpoint {@link /transactions/transactions-by-account/me} on transactions service.
 * In order to phase out transactions service, this endpoint is replicated here, because the clients for France still keeps calling it.
 * <p>
 * The Swagger annotations are left out on purpose because this is not a public API.
 */
@Slf4j
@RequiredArgsConstructor
@RestController
public class LegacyTransactionController {

    private final LegacyTransactionService legacyTransactionService;

    @GetMapping(value = "/legacy-transactions/transactions-by-account/me", produces = APPLICATION_JSON_VALUE)
    public ResponseEntity<LegacyTransactionsByAccountDTO> getTransactionsByAccount(
            @VerifiedClientToken ClientUserToken clientUserToken,
            @RequestParam("accountId")
                    UUID accountId,
            @Nullable @RequestParam(required = false)
                    DateInterval dateInterval,
            @Nullable @RequestParam(required = false)
                    String next
    ) {
        LegacyTransactionsByAccountDTO transactionsDTO = legacyTransactionService.getTransactions(clientUserToken, accountId, dateInterval, next);
        var sorted = transactionsDTO.getTransactions().stream()
                .sorted(comparing(LegacyTransactionDTO::getDate))
                .collect(toList());
        log.info("GET /legacy-transactions/transactions-by-account/me?accountId={}&dateInterval={}&next={} -> n={}, lb={}, ub={}, hasNext={}"
                , accountId
                , dateInterval
                , next != null // This is a base64 encoded string, no need to log it in full, just log if it's present/absent
                , sorted.size() // n = amount of trxs returned
                , !sorted.isEmpty() ? sorted.get(0).getDate() : null // lb_date = the lowest date in batch
                , !sorted.isEmpty() ? sorted.get(sorted.size() - 1).getDate() : null // ub_date = highest date in response
                , transactionsDTO.getLinks().getNext() != null
        ); // NOSHERIFF
        return ResponseEntity.ok(transactionsDTO);
    }
}
