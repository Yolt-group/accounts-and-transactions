package com.yolt.accountsandtransactions.tools;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import nl.ing.lovebird.extendeddata.transaction.ExtendedTransactionDTO;
import nl.ing.lovebird.providerdomain.ProviderTransactionDTO;
import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This is not a test but a tool to help you pseudonymize a list of transactions.
 *
 * Use the {@link #main(String[])} method and change the variable inputPath, that should get you up to speed.
 * Check out the {@link #testAnonymizeTransaction()} with a debugger to see how it works.
 *
 * **IMPORTANT** note that only fields that I needed when using this tool were added to {@link #anonymizeTransaction}.
 *               ALWAYS manually check the output you get from this program before committing it to source control.
 */
public class ProviderTransactionAnonymizer {

    static ObjectMapper objectMapper = createObjectMapper();
    static Mac mac;
    static byte[] secret = null;

    public static void main(String [] args) throws IOException {
//        String inputPath = "/transactionmatching/barclays/2020-03-30T16:03.anon.json";
        String inputPath = "/transactionmatching/barclays/2020-03-31T04:34.anon.json";

        initmac();
        InputStream input = ProviderTransactionAnonymizer.class.getResourceAsStream(inputPath);
        List<ProviderTransactionDTO> nonAnonymizedTransactions = objectMapper.readValue(input.readAllBytes(), new TypeReference<List<ProviderTransactionDTO>>() {
        });
        List<ProviderTransactionDTO> actualAnonymizedTransactions = nonAnonymizedTransactions.stream()
                .map(ProviderTransactionAnonymizer::anonymizeTransaction)
                .collect(Collectors.toList());

        System.out.println(objectMapper.writeValueAsString(actualAnonymizedTransactions));
    }

    @Test
    public void testAnonymizeTransaction() throws IOException {
        secret = new byte[]{0};
        initmac();
        InputStream input = ProviderTransactionAnonymizer.class.getResourceAsStream("/tools/anonymizer/test-input.json");
        InputStream expectedOutput = ProviderTransactionAnonymizer.class.getResourceAsStream("/tools/anonymizer/expected-output.json");

        List<ProviderTransactionDTO> nonAnonymizedTransactions = objectMapper.readValue(input.readAllBytes(), new TypeReference<List<ProviderTransactionDTO>>() {
        });
        List<ProviderTransactionDTO> expectedAnonymizedTransactions = objectMapper.readValue(expectedOutput.readAllBytes(), new TypeReference<List<ProviderTransactionDTO>>() {
        });

        List<ProviderTransactionDTO> actualAnonymizedTransactions = nonAnonymizedTransactions.stream()
                .map(ProviderTransactionAnonymizer::anonymizeTransaction)
                .collect(Collectors.toList());

        System.out.println(objectMapper.writeValueAsString(actualAnonymizedTransactions));

        Assert.assertEquals(
                objectMapper.writeValueAsString(expectedAnonymizedTransactions),
                objectMapper.writeValueAsString(actualAnonymizedTransactions)
        );
    }

    private static ProviderTransactionDTO anonymizeTransaction(ProviderTransactionDTO trx) {
        final ExtendedTransactionDTO extTrx = trx.getExtendedTransaction();
        return trx.toBuilder()
                .description(anonymize(trx.getDescription()))
                .merchant(anonymize(trx.getMerchant()))
                .extendedTransaction(extTrx.toBuilder()
                        .remittanceInformationStructured(anonymize(extTrx.getRemittanceInformationStructured()))
                        .remittanceInformationUnstructured(anonymize(extTrx.getRemittanceInformationUnstructured()))
                        .build())
                .build();
    }

    private static String anonymize(String input) {
        if (input == null) {
            return null;
        }
        return Base64.encodeBase64String(mac.doFinal(input.getBytes()));
    }

    private static byte[] getRandomBits(int bits) {
        if (bits % 8 != 0) {
            throw new IllegalArgumentException();
        }
        SecureRandom secureRandom = new SecureRandom();
        byte[] secret = new byte[bits / 8];
        secureRandom.nextBytes(secret);
        System.err.println("Remember the below secret if you want to re-use this for another batch:\n" + Arrays.toString(secret));
        return secret;
    }

    static ObjectMapper createObjectMapper() {
        Jackson2ObjectMapperBuilder jacksonObjectMapperBuilder = new Jackson2ObjectMapperBuilder();
        ((Jackson2ObjectMapperBuilderCustomizer) builder -> builder.featuresToDisable(
                JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION,
                SerializationFeature.WRITE_DATES_AS_TIMESTAMPS,
                DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                DeserializationFeature.ADJUST_DATES_TO_CONTEXT_TIME_ZONE)).customize(jacksonObjectMapperBuilder);
        ObjectMapper res = jacksonObjectMapperBuilder.build();
        res.findAndRegisterModules();
        return res;
    }

    static void initmac() {
        if (secret == null) {
            secret = getRandomBits(128);
        }
        try {
            mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec secret_key = new SecretKeySpec(secret, "HmacSHA256");
            mac.init(secret_key);
        } catch (NoSuchAlgorithmException | InvalidKeyException e) {
            e.printStackTrace();
        }
    }
}
