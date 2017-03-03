package org.icatproject.lucene;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.icatproject.utils.AddressChecker;
import org.icatproject.utils.AddressCheckerException;
import org.icatproject.utils.CheckedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Provider
public class AddressFilter implements ContainerRequestFilter {

	private static Logger logger = LoggerFactory.getLogger(AddressFilter.class);

	private static AddressChecker addressChecker;

	private static final Marker fatal = MarkerFactory.getMarker("FATAL");

	static {
		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromResource("run.properties");
			addressChecker = new AddressChecker(props.getString("ip"));
		} catch (Exception e) {
			logger.error(fatal, e.getMessage());
			throw new IllegalStateException(e.getMessage());
		}
	}

	@Context
	private HttpServletRequest servletRequest;

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		String ip = servletRequest.getRemoteAddr();
		try {
			if (addressChecker.check(ip)) {
				return;
			}
		} catch (AddressCheckerException e) {
			logger.error("AddressChecker reports: {}", e.getMessage());
			requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED).entity(e.getMessage()).build());
		}

		logger.warn("Access from this IP address is not allowed {}.", ip);
		requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
				.entity("Access from this IP address is not allowed").build());
	}

}