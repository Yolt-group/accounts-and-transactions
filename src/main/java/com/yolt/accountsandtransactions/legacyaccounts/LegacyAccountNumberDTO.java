package com.yolt.accountsandtransactions.legacyaccounts;

import com.yolt.accountsandtransactions.accounts.Account;
import lombok.Builder;
import lombok.Value;

import javax.validation.constraints.Size;

@Value
@Builder
public class LegacyAccountNumberDTO {

    // Legal name of account holder; e.g. "Samuel Martha Smith"
    @Size(max = 256)
    String holderName;
    // accountHolder on AccountDTO

    // Scheme for identification field; e.g. IBAN or SORTCODEACCOUNTNUMBER (required = true)
    Scheme scheme;

    // The actual account number; Naming convention taken from OpenBanking specification (required = true)
    @Size(max = 256)
    String identification;

    // Optional secondary identification. Supports the roll number for building societies in OpenBanking.
    @Size(max = 256)
    String secondaryIdentification;

    public enum Scheme {
        /**
         * The International Bank Account Number (IBAN) is an internationally agreed system of identifying bank accounts across national
         * borders to facilitate the communication and processing of cross border transactions with a reduced risk of transcription errors.
         * https://en.wikipedia.org/wiki/International_Bank_Account_Number
         */
        IBAN,
        /**
         * Sort Codes are a unique number assigned to branches of banks and financial institutions in the United Kingdom and Ireland. United
         * Kingdom's sort codes and bank accounts system provides a number of internal checksum validation methods which are used to verify
         * if a bank account number is correctly composed.
         * https://en.wikipedia.org/wiki/Sort_code
         */
        SORTCODEACCOUNTNUMBER
    }

    public static LegacyAccountNumberDTO from(final Account account) {
        if (account.getIban() != null) {
            return LegacyAccountNumberDTO.builder()
                    .scheme(Scheme.IBAN)
                    .identification(account.getIban())
                    .holderName(account.getAccountHolder())
                    .build();
        } else if (account.getSortCodeAccountNumber() != null) {
            return LegacyAccountNumberDTO.builder()
                    .scheme(Scheme.SORTCODEACCOUNTNUMBER)
                    .identification(account.getSortCodeAccountNumber())
                    .holderName(account.getAccountHolder())
                    .build();
        }

        return null;
    }
}
