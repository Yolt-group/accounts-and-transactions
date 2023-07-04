package com.yolt.accountsandtransactions.legacytransactions;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import org.springframework.web.util.UriComponentsBuilder;

@Getter
@AllArgsConstructor
@NoArgsConstructor
@Schema
public class LegacyMerchantDTO {
    public static final UriComponentsBuilder MERCHANT_IMAGE_ROUTE_TEMPLATE
            = UriComponentsBuilder.fromPath("/content/images/merchants/icons/nl/{name}.png");

    private String name;

    @Schema(required = true)
    @JsonProperty("_links")
    private LinksDTO links;

    public static LegacyMerchantDTO from(@NonNull final String name) {
        String merchantIconUri = MERCHANT_IMAGE_ROUTE_TEMPLATE.buildAndExpand(name).toUriString();
        return new LegacyMerchantDTO(name, new LinksDTO(new LinkDTO(merchantIconUri)));
    }

    @Data
    @AllArgsConstructor
    @Schema
    public static class LinksDTO {
        private LinkDTO icon;
    }
}
