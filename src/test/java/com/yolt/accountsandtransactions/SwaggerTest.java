package com.yolt.accountsandtransactions;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class SwaggerTest extends BaseIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    public void when_IRequestSwaggerThroughTheAPI_then_swaggerShouldBeReturned() throws Exception {
        this.mockMvc.perform(get("/v3/api-docs"))
                .andExpect(status().is(200));
                // .andDo(res -> System.out.println(res.getResponse().getContentAsString()));
    }
}
