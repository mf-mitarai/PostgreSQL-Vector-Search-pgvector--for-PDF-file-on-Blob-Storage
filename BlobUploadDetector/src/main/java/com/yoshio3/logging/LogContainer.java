package com.yoshio3.logging;

import com.microsoft.azure.functions.ExecutionContext;
import com.yoshio3.CosmosDBUtil;
import com.yoshio3.Function;

public record LogContainer(BDLogger funcLogger, BDLogger cosmosLogger) {
	public static LogContainer create(final ExecutionContext context) {
		return new LogContainer(BDLogFactory.getLogger(context, Function.class), BDLogFactory.getLogger(context, CosmosDBUtil.class));
	}
}
