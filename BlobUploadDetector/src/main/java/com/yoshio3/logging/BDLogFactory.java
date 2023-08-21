package com.yoshio3.logging;

import com.microsoft.azure.functions.ExecutionContext;

public final class BDLogFactory {

	public static BDLogger getLogger(final ExecutionContext context, Class<?> clazz) {
		return new BDLogger(context, clazz);
	}

}
