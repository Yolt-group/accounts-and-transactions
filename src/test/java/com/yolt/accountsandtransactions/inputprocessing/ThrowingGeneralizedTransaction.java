package com.yolt.accountsandtransactions.inputprocessing;

import com.yolt.accountsandtransactions.inputprocessing.matching.GeneralizedTransaction;
import lombok.NonNull;
import nl.ing.lovebird.extendeddata.transaction.TransactionStatus;
import org.apache.commons.lang3.NotImplementedException;
import org.springframework.lang.Nullable;

import java.time.Instant;
import java.time.LocalDate;

public class ThrowingGeneralizedTransaction implements GeneralizedTransaction {

    @Nullable
    @Override
    public String getInternalId() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getExternalId() {
        throw new NotImplementedException();
    }

    @Override
    public @NonNull Long getAmountInCents() {
        throw new NotImplementedException();
    }

    @Override
    public @NonNull LocalDate getDate() {
        throw new NotImplementedException();
    }

    @Override
    public @NonNull TransactionStatus getStatus() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public LocalDate getBookingDate() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public Instant getTimestamp() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getDebtorName() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getDebtorAccountNr() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getCreditorName() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getCreditorAccountNr() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getDescription() {
        throw new NotImplementedException();
    }

    @Nullable
    @Override
    public String getEndToEndId() {
        throw new NotImplementedException();
    }
}
