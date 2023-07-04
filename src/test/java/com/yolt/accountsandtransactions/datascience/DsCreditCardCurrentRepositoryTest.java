package com.yolt.accountsandtransactions.datascience;


import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.yolt.accountsandtransactions.BaseIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.yolt.accountsandtransactions.datascience.DsTransaction.USER_ID_COLUMN;
import static com.yolt.accountsandtransactions.datascience.TestDsCreditCardCurrentBuilder.testDsCreditCardCurrentBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class DsCreditCardCurrentRepositoryTest extends BaseIntegrationTest {

    @Autowired
    private DsCreditCardsCurrentRepository repository;

    private Mapper<DSCreditCardCurrent> mapper;


    @Override
    protected void setup() {

        mapper = mappingManager.mapper(DSCreditCardCurrent.class, dsKeyspace);
    }

    @Test
    public void shouldBeAbleToInsertAndOverwriteACreditCardForAUser() {
        // Given
        final Date lastUpdatedTime2 = Date.from(ZonedDateTime.of(2012, 2, 2, 1, 1, 1, 1, ZoneId.of("UTC")).toInstant());
        final DSCreditCardCurrent creditCard1 = testDsCreditCardCurrentBuilder().userId(new UUID(0, 1)).build();
        final DSCreditCardCurrent creditCard2 = testDsCreditCardCurrentBuilder().userId(new UUID(0, 1)).lastUpdatedTime(lastUpdatedTime2).build();
        final DSCreditCardCurrent creditCard3 = testDsCreditCardCurrentBuilder().userId(new UUID(0, 2)).build();
        final DSCreditCardCurrent creditCard4 = testDsCreditCardCurrentBuilder().userId(new UUID(0, 2)).lastUpdatedTime(lastUpdatedTime2).build();

        // When
        repository.save(creditCard1);
        repository.save(creditCard2);
        repository.save(creditCard3);
        repository.save(creditCard4);

        // Then
        Select select = QueryBuilder.select().from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME);
        select.where(eq(USER_ID_COLUMN, new UUID(0, 1)));
        List<DSCreditCardCurrent> creditCardsCurrent = mapper.map(session.execute(select)).all();

        assertThat(creditCardsCurrent).hasSize(1);
        assertThat(creditCardsCurrent.get(0)).isEqualToComparingFieldByField(creditCard2);

        select = QueryBuilder.select().from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME);
        select.where(eq(USER_ID_COLUMN, new UUID(0, 2)));
        creditCardsCurrent = mapper.map(session.execute(select)).all();

        assertThat(creditCardsCurrent).hasSize(1);
        assertThat(creditCardsCurrent.get(0)).isEqualToComparingFieldByField(creditCard4);
    }

    @Test
    public void shouldNotWriteNullValues() {
        // Given
        final DSCreditCardCurrent dsCreditCard = testDsCreditCardCurrentBuilder().build();
        repository.save(dsCreditCard);
        Select select = QueryBuilder.select().from(dsKeyspace, DSCreditCardCurrent.TABLE_NAME);
        select.where(eq(USER_ID_COLUMN, new UUID(0, 1)));
        DSCreditCardCurrent actualDsCreditCard = mapper.map(session.execute(select)).one();

        // Then
        assertThat(actualDsCreditCard.getExternalAccountId()).isEqualTo(dsCreditCard.getExternalAccountId());
        assertThat(actualDsCreditCard.getExternalSiteId()).isEqualTo(dsCreditCard.getExternalSiteId());

        // And when
        actualDsCreditCard.setExternalAccountId(null);
        actualDsCreditCard.setExternalSiteId(null);
        repository.save(actualDsCreditCard);

        // Then nulls were not writen
        actualDsCreditCard = mapper.map(session.execute(select)).one();
        assertThat(actualDsCreditCard.getExternalAccountId()).isEqualTo(dsCreditCard.getExternalAccountId());
        assertThat(dsCreditCard.getExternalSiteId()).isEqualTo(dsCreditCard.getExternalSiteId());
    }
}
