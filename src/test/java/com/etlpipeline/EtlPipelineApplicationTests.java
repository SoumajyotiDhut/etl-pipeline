package com.etlpipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class EtlPipelineApplicationTests {

	@Test
	@DisplayName("Application context loads successfully")
	void contextLoads() {
		// This test verifies the entire Spring context
		// starts without errors — all beans wire correctly
	}
}
