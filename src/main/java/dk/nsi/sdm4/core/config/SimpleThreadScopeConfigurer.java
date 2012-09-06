package dk.nsi.sdm4.core.config;

import org.springframework.beans.factory.config.CustomScopeConfigurer;
import org.springframework.context.support.SimpleThreadScope;

import java.util.HashMap;

public class SimpleThreadScopeConfigurer extends CustomScopeConfigurer {
	public SimpleThreadScopeConfigurer() {
		super();
		super.setScopes(new HashMap<String, Object>() {
			{
				put("thread", new SimpleThreadScope());
			}
		});
	}
}
