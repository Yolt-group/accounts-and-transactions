package com.yolt.accountsandtransactions.accounts;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Value;

@Value
@JsonInclude(JsonInclude.Include.NON_EMPTY)
@Schema(description = "An account reference. If this object is present, at least one of the sub properties is given")
public class AccountReferencesDTO {
    @Schema(description = "The International Bank Account Number (IBAN).", example = "NL79ABNA9455762838")
    String iban;
    @Schema(description = "The masked Permanent Account Number (PAN).", example = "XXXXXXXXXX1234")
    String maskedPan;
    @Schema(description = "The Permanent Account Number (PAN).", example = "1111 1111 1111 1111")
    String pan;
    @Schema(description = "The  Basic Bank Account Number (BBAN).", example = "5390 0754 7034")
    String bban;
    @Schema(description = "The sort code and account number. UK specific.", example = "12-34-5612345678")
    String sortCodeAccountNumber;
}