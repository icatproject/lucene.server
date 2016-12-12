package org.icatproject.lucene;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.Singleton;
import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import javax.json.stream.JsonGenerator;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;

import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler.ConfigurationKeys;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BooleanQuery.Builder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.search.join.JoinUtil;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.icatproject.lucene.exceptions.LuceneException;
import org.icatproject.utils.CheckedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

@Path("/")
@Singleton
public class Lucene {

	public class Search {
		public Map<String, IndexSearcher> map;
		public Query query;
		public ScoreDoc lastDoc;
	}

	private static final Logger logger = LoggerFactory.getLogger(Lucene.class);
	private static final Marker fatal = MarkerFactory.getMarker("FATAL");

	private String luceneDirectory;
	private int luceneCommitMillis;
	private AtomicLong bucketNum = new AtomicLong();

	private class IndexBucket {
		private FSDirectory directory;
		private IndexWriter indexWriter;
		private SearcherManager searcherManager;
	}

	private Map<String, IndexBucket> indexBuckets = new ConcurrentHashMap<>();
	private Map<String, String> uniqueStrings = new ConcurrentHashMap<>();

	private StandardQueryParser parser;

	private Timer timer;
	private IcatAnalyzer analyzer;
	private AtomicReference<String> populatingName;
	private Set<Document> docsToAdd = new HashSet<>();
	private Map<Long, Search> searches = new ConcurrentHashMap<>();

	@PreDestroy
	private void exit() {
		logger.info("Closing down icat.lucene");
		timer.cancel();
		timer = null; // This seems to be necessary to make it really stop
		try {
			for (IndexBucket bucket : indexBuckets.values()) {
				bucket.searcherManager.close();
				bucket.indexWriter.commit();
				bucket.indexWriter.close();
				bucket.directory.close();
			}
			logger.info("Closed down icat.lucene");
		} catch (Exception e) {
			logger.error(fatal, "Problem closing down icat.lucene", e);
		}
	}

	@PostConstruct
	private void init() {
		logger.info("Initialising icat.lucene");
		CheckedProperties props = new CheckedProperties();
		try {
			props.loadFromFile("lucene.properties");

			luceneDirectory = props.getString("directory");

			luceneCommitMillis = props.getPositiveInt("commitSeconds") * 1000;

			analyzer = new IcatAnalyzer();

			parser = new StandardQueryParser();
			StandardQueryConfigHandler qpConf = (StandardQueryConfigHandler) parser.getQueryConfigHandler();
			qpConf.set(ConfigurationKeys.ANALYZER, analyzer);
			qpConf.set(ConfigurationKeys.ALLOW_LEADING_WILDCARD, true);

			timer = new Timer("LuceneCommitTimer");
			timer.schedule(new TimerTask() {

				@Override
				public void run() {
					try {
						commit();
					} catch (Throwable t) {
						logger.error(t.getMessage());
					}
				}
			}, luceneCommitMillis, luceneCommitMillis);
			populatingName = new AtomicReference<String>();

		} catch (Exception e) {
			logger.error(fatal, e.getMessage());
			throw new IllegalStateException(e.getMessage());
		}

		logger.info("Initialised icat.lucene");
	}

	@POST
	@Path("commit")
	public void commit() throws LuceneException {
		logger.debug("Requesting commit");
		try {
			for (Entry<String, IndexBucket> entry : indexBuckets.entrySet()) {
				IndexBucket bucket = entry.getValue();
				int cached = bucket.indexWriter.numRamDocs();
				bucket.indexWriter.commit();
				if (cached != 0) {
					logger.debug("Synch has committed {} {} changes to Lucene - now have {} documents indexed", cached,
							entry.getKey(), bucket.indexWriter.numDocs());
				}
				bucket.searcherManager.maybeRefreshBlocking();
			}
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datafiles")
	public String datafiles(@Context HttpServletRequest request, @QueryParam("maxResults") int maxResults)
			throws LuceneException {
		Long uid = null;
		try {
			uid = bucketNum.getAndIncrement();
			Search search = new Search();
			searches.put(uid, search);
			Map<String, IndexSearcher> map = new HashMap<>();
			search.map = map;

			try (JsonReader r = Json.createReader(request.getInputStream())) {
				JsonObject o = r.readObject();
				String userName = o.getString("user", null);

				BooleanQuery.Builder theQuery = new BooleanQuery.Builder();

				if (userName != null) {
					Query iuQuery = JoinUtil.createJoinQuery("investigation", false, "id",
							new TermQuery(new Term("name", userName)), getSearcher(map, "InvestigationUser"),
							ScoreMode.None);

					Query invQuery = JoinUtil.createJoinQuery("id", false, "investigation", iuQuery,
							getSearcher(map, "Investigation"), ScoreMode.None);

					Query dsQuery = JoinUtil.createJoinQuery("id", false, "dataset", invQuery,
							getSearcher(map, "Dataset"), ScoreMode.None);

					theQuery.add(dsQuery, Occur.MUST);
				}

				String text = o.getString("text", null);
				if (text != null) {
					theQuery.add(parser.parse(text, "text"), Occur.MUST);
				}

				String lower = o.getString("lower", null);
				String upper = o.getString("upper", null);
				if (lower != null && upper != null) {
					theQuery.add(new TermRangeQuery("date", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
					theQuery.add(new TermRangeQuery("date", new BytesRef(lower), new BytesRef(upper), true, true),
							Occur.MUST);
				}

				if (o.containsKey("params")) {
					JsonArray params = o.getJsonArray("params");
					IndexSearcher datafileParameterSearcher = getSearcher(map, "DatafileParameter");
					for (JsonValue p : params) {
						JsonObject parameter = (JsonObject) p;

						BooleanQuery.Builder paramQuery = new BooleanQuery.Builder();
						String pName = parameter.getString("name", null);
						if (pName != null) {
							paramQuery.add(new WildcardQuery(new Term("name", pName)), Occur.MUST);
						}
						String pUnits = parameter.getString("units", null);
						if (pUnits != null) {
							paramQuery.add(new WildcardQuery(new Term("units", pUnits)), Occur.MUST);
						}
						String pStringValue = parameter.getString("stringValue", null);
						String pLowerDateValue = parameter.getString("lowerDateValue", null);
						String pUpperDateValue = parameter.getString("upperDateValue", null);
						Double pLowerNumericValue = parameter.containsKey("lowerNumericValue")
								? parameter.getJsonNumber("lowerNumericValue").doubleValue() : null;
						Double pUpperNumericValue = parameter.containsKey("upperNumericValue")
								? parameter.getJsonNumber("upperNumericValue").doubleValue() : null;
						if (pStringValue != null) {
							paramQuery.add(new WildcardQuery(new Term("stringValue", pStringValue)), Occur.MUST);
						} else if (pLowerDateValue != null && pUpperDateValue != null) {
							paramQuery.add(new TermRangeQuery("dateTimeValue", new BytesRef(pLowerDateValue),
									new BytesRef(upper), true, true), Occur.MUST);

						} else if (pLowerNumericValue != null && pUpperNumericValue != null) {
							paramQuery.add(NumericRangeQuery.newDoubleRange("numericValue", pLowerNumericValue,
									pUpperNumericValue, true, true), Occur.MUST);
						}
						Query toQuery = JoinUtil.createJoinQuery("datafile", false, "id", paramQuery.build(),
								datafileParameterSearcher, ScoreMode.None);
						theQuery.add(toQuery, Occur.MUST);
					}
				}
				search.query = maybeEmptyQuery(theQuery);
			}

			return luceneSearchResult("Datafile", search, maxResults, uid);
		} catch (Exception e) {
			logger.error("Error", e);
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("datafiles/{uid}")
	public String datafilesAfter(@PathParam("uid") long uid, @QueryParam("maxResults") int maxResults)
			throws LuceneException {
		try {
			Search search = searches.get(uid);
			try {
				return luceneSearchResult("Datafile", search, maxResults, null);
			} catch (Exception e) {
				throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
			}
		} catch (Exception e) {
			freeSearcher(uid);
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private Query maybeEmptyQuery(Builder theQuery) {
		Query query = theQuery.build();
		if (query.toString().isEmpty()) {
			query = new MatchAllDocsQuery();
		}
		logger.debug("Lucene query {}", query);
		return query;
	}

	private String luceneSearchResult(String name, Search search, int maxResults, Long uid) throws IOException {
		IndexSearcher isearcher = getSearcher(search.map, name);
		logger.debug("To search {} {} with {} from {} ", search.query, maxResults, isearcher, search.lastDoc);
		TopDocs topDocs = search.lastDoc == null ? isearcher.search(search.query, maxResults)
				: isearcher.searchAfter(search.lastDoc, search.query, maxResults);
		ScoreDoc[] hits = topDocs.scoreDocs;
		logger.debug("Hits " + topDocs.totalHits + " maxscore " + topDocs.getMaxScore());
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (JsonGenerator gen = Json.createGenerator(baos)) {
			gen.writeStartObject();
			if (uid != null) {
				gen.write("uid", uid);
			}
			gen.writeStartArray("results");
			for (ScoreDoc hit : hits) {
				Document doc = isearcher.doc(hit.doc);
				gen.writeStartArray();
				gen.write(Long.parseLong(doc.get("id")));
				gen.write(hit.score);
				gen.writeEnd(); // array
			}
			gen.writeEnd(); // array results
			gen.writeEnd(); // object
		}

		search.lastDoc = hits.length == 0 ? null : hits[hits.length - 1];
		logger.debug("Json returned {}", baos.toString());
		return baos.toString();
	}

	@DELETE
	@Path("freeSearcher/{uid}")
	public void freeSearcher(@PathParam("uid") Long uid) throws LuceneException {
		if (uid != null) { // May not be set for internal calls
			logger.debug("Requesting freeSearcher {}", uid);
			Map<String, IndexSearcher> search = searches.get(uid).map;
			for (Entry<String, IndexSearcher> entry : search.entrySet()) {
				String name = entry.getKey();
				IndexSearcher isearcher = entry.getValue();
				SearcherManager manager = indexBuckets.computeIfAbsent(name, k -> createBucket(k)).searcherManager;
				try {
					manager.release(isearcher);
				} catch (IOException e) {
					throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
				}
			}
			searches.remove(uid);
		}
	}

	/*
	 * Need a new set of IndexSearchers for each search as identified by a uid
	 */
	private IndexSearcher getSearcher(Map<String, IndexSearcher> bucket, String name) throws IOException {
		IndexSearcher isearcher = bucket.get(name);
		if (isearcher == null) {
			isearcher = indexBuckets.computeIfAbsent(name, k -> createBucket(k)).searcherManager.acquire();
			bucket.put(name, isearcher);
			logger.debug("Remember searcher for {}", name);
		}
		return isearcher;
	}

	enum AttributeName {
		type, name, value, date, store
	}

	enum FieldType {
		TextField, StringField
	}

	enum When {
		Now, Sometime
	}

	/*
	 * Expect an array of documents each encoded as an array of things to add to
	 * the document
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("addNow/{entityName}")
	public void addNow(@Context HttpServletRequest request, @PathParam("entityName") String entityName)
			throws LuceneException {
		logger.debug("Requesting addNow of {}", entityName);
		lock(entityName);
		int count = 0;
		try (JsonParser parser = Json.createParser(request.getInputStream())) {
			Event ev = parser.next(); // Opening [
			while (true) {
				ev = parser.next(); // Final ] or another document
				if (ev == Event.END_ARRAY) {
					break;
				}
				add(request, entityName, When.Now, parser);
				count++;
			}

		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		} finally {
			unlock(entityName);
		}
		commit();
		logger.debug("Added {} {} documents", count, entityName);
	}

	/*
	 * Expect an of things to add to a single document
	 */
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Path("add/{entityName}")
	public void add(@Context HttpServletRequest request, @PathParam("entityName") String entityName)
			throws LuceneException {
		logger.debug("Requesting add of {}", entityName);
		try (JsonParser parser = Json.createParser(request.getInputStream())) {
			parser.next(); // Skip the opening [
			add(request, entityName, When.Sometime, parser);
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	private void add(HttpServletRequest request, String entityName, When when, JsonParser parser)
			throws LuceneException, IOException {

		IndexWriter indexWriter = indexBuckets.computeIfAbsent(entityName, k -> createBucket(k)).indexWriter;

		AttributeName attName = null;
		FieldType fType = null;
		String name = null;
		String value = null;
		Store store = Store.NO;
		Document doc = new Document();

		parser.next(); // Skip the [
		while (parser.hasNext()) {
			Event ev = parser.next();
			if (ev == Event.KEY_NAME) {
				try {
					attName = AttributeName.valueOf(parser.getString());
				} catch (Exception e) {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
							"Found unknown field type " + e.getMessage());
				}
			} else if (ev == Event.VALUE_STRING) {
				if (attName == AttributeName.type) {
					try {
						fType = FieldType.valueOf(parser.getString());
					} catch (Exception e) {
						throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST,
								"Found unknown field type " + e.getMessage());
					}
				} else if (attName == AttributeName.name) {
					name = parser.getString();
				} else if (attName == AttributeName.value) {
					value = parser.getString();
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_STRING " + attName);
				}
			} else if (ev == Event.VALUE_NUMBER) {
				if (attName == AttributeName.date) {
					value = DateTools.dateToString(new Date(parser.getLong()), Resolution.MINUTE);
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_NUMBER " + attName);
				}
			} else if (ev == Event.VALUE_TRUE) {
				if (attName == AttributeName.store) {
					store = Store.YES;
				} else {
					throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Bad VALUE_TRUE " + attName);
				}
			} else if (ev == Event.START_OBJECT) {
				fType = null;
				name = null;
				value = null;
				store = Store.NO;
			} else if (ev == Event.END_OBJECT) {
				if (fType == FieldType.TextField) {
					doc.add(new TextField(name, value, store));
				} else if (fType == FieldType.StringField) {
					doc.add(new StringField(name, value, store));
				}

			} else if (ev == Event.END_ARRAY) {
				if (entityName.equals(populatingName.get()) && when == When.Sometime) {
					docsToAdd.add(doc);
					logger.trace("Will add to {} lucene index later", entityName);
				} else {
					indexWriter.addDocument(doc);
				}
				return;
			} else {
				throw new LuceneException(HttpURLConnection.HTTP_BAD_REQUEST, "Unexpected token in Json: " + ev);
			}
		}
	}

	@DELETE
	@Path("deleteAll/{entityName}")
	public void deleteAll(@PathParam("entityName") String entityName) throws LuceneException {
		logger.info("Requesting delete of all {}", entityName);
		try {
			indexBuckets.computeIfAbsent(entityName, k -> createBucket(k)).indexWriter.deleteAll();
		} catch (IOException e) {
			throw new LuceneException(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
		}
	}

	// TODO Make private
	@POST
	@Path("lock/{entityName}")
	public void lock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.info("Requesting lock of {} index", entityName);
		String fixedName = uniqueStrings.computeIfAbsent(entityName, s -> s);
		if (!populatingName.compareAndSet(null, fixedName)) {
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
					"Lucene already locked by " + populatingName);
		}
		logger.trace("Now set to {}", populatingName);
	}

	// TODO Make private
	@POST
	@Path("unlock/{entityName}")
	public void unlock(@PathParam("entityName") String entityName) throws LuceneException {
		logger.debug("Requesting unlock of {} index", entityName);
		String fixedName = uniqueStrings.computeIfAbsent(entityName, s -> s);
		if (!populatingName.compareAndSet(fixedName, null)) {
			if (populatingName.get() == null) {
				throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE, "Lucene is not currently locked");
			}
			throw new LuceneException(HttpURLConnection.HTTP_NOT_ACCEPTABLE,
					"Lucene currently locked by " + populatingName);
		}
		/* Process the ones that came in while populating */
		// TODO - server side Class<?> klass =
		// Class.forName(Constants.ENTITY_PREFIX +
		// populatingClassName);
		// for (Long id : idsToCheck) {
		// EntityBaseBean bean = (EntityBaseBean)
		// manager.find(klass, id);
		// if (bean != null) {
		// Document doc = bean.getDoc();
		// iwriter.updateDocument(new Term("id", id.toString()),
		// doc);
		// } else {
		// iwriter.deleteDocuments(new Term("id",
		// id.toString()));
		// }
		// }

		logger.trace("Now set to {}", populatingName);
	}

	private IndexBucket createBucket(String name) {
		try {
			IndexBucket bucket = new IndexBucket();
			FSDirectory directory = FSDirectory.open(Paths.get(luceneDirectory, name));
			bucket.directory = directory;
			logger.debug("Opened FSDirectory {}", directory);

			IndexWriterConfig config = new IndexWriterConfig(analyzer);
			IndexWriter iwriter = new IndexWriter(directory, config);
			String[] files = directory.listAll();
			if (files.length == 1 && files[0].equals("write.lock")) {
				logger.debug("Directory only has the write.lock file so store and delete a dummy document");
				Document doc = new Document();
				doc.add(new StringField("dummy", "dummy", Store.NO));
				iwriter.addDocument(doc);
				iwriter.commit();
				iwriter.deleteDocuments(new Term("dummy", "dummy"));
				iwriter.commit();
				logger.debug("Now have " + iwriter.numDocs() + " documents indexed");
			}
			bucket.indexWriter = iwriter;
			bucket.searcherManager = new SearcherManager(iwriter, false, null);
			logger.debug("Bucket for {} is now ready", name);
			return bucket;
		} catch (Throwable e) {
			logger.error("Can't continue " + e.getClass() + " " + e.getMessage());
			return null;
		}
	}

}
