package org.icatproject.lucene.exceptions;

import java.io.ByteArrayOutputStream;

import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class LuceneExceptionMapper implements ExceptionMapper<LuceneException> {

	@Override
	public Response toResponse(LuceneException e) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		JsonGenerator gen = Json.createGenerator(baos);
		gen.writeStartObject().write("code", e.getClass().getSimpleName()).write("message", e.getShortMessage());
		gen.writeEnd().close();
		return Response.status(e.getHttpStatusCode()).entity(baos.toString()).build();
	}
}