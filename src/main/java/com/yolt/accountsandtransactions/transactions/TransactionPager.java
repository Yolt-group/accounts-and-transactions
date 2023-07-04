package com.yolt.accountsandtransactions.transactions;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.PagingStateException;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class TransactionPager<T extends Transaction> {
    private final Session session;
    private final Mapper<T> mapper;

    public TransactionPager(final Session session, final Class<T> type) {
        this.session = session;
        this.mapper = new MappingManager(session).mapper(type);
    }

    public TransactionsPage getPage(final Select regularSelect, final String pagingState) {
        if (pagingState != null) {
            try {
                regularSelect.setPagingState(PagingState.fromString(pagingState));
            } catch (PagingStateException e) {
                log.error("Invalid paging state {} for query {}", pagingState, regularSelect.getQueryString(), e);
                throw new InvalidPagingStateException(String.format("Invalid paging state %s for query %s", pagingState, regularSelect.getQueryString()));
            }
        }

        final ResultSet resultSet = session.execute(regularSelect);

        final PagingState newPagingState = resultSet.getExecutionInfo().getPagingState();
        int remaining = resultSet.getAvailableWithoutFetching();

        final List<Transaction> transactions = new ArrayList<>(remaining);
        final Result<T> result = mapper.map(resultSet);
        for (T trx : result) {
            transactions.add(trx);

            // Prevent the driver from retrieving more data (which would happen we keep calling converter.read())
            --remaining;
            if (remaining == 0) {
                break;
            }
        }

        String serializedNewPagingState = newPagingState != null ? newPagingState.toString() : null;

        return new TransactionsPage(transactions, serializedNewPagingState);
    }

}
