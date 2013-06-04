package nl.vpro.couchdb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * An iterator over a couchdb view that is streaming, which doesn't use any memory, so you can use for huge results.
 *
 * @author Michiel Meeuwissen
 * @since 1.3
 */
public class CouchdbViewIterator implements Iterator<JsonNode> {

    private final JsonParser parser;

    private JsonNode next;

    private int depth = 0;

    public CouchdbViewIterator(InputStream is) throws IOException {
        JsonFactory jsonFactory = JacksonMapper.INSTANCE.getJsonFactory();
        this.parser = jsonFactory.createJsonParser(is);
    }

    @Override
    public boolean hasNext() {
        findNext();
        return next != null;
    }

    @Override
    public JsonNode next() {
        findNext();
        if(next == null) {
            throw new NoSuchElementException();
        }
        JsonNode result = next;
        next = null;
        return result;
    }

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	private void findNext() {
        if(next == null) {
            try {
                while(true) {
                    JsonToken token = parser.nextToken();
                    if(token == null) {
                        break;
                    }
                    if(token == JsonToken.START_OBJECT) {
                        depth++;
                    }
                    if(token == JsonToken.END_OBJECT) {
                        depth--;
                    }
                    if(token == JsonToken.START_OBJECT && depth == 2) {
                        next = parser.readValueAsTree();
                        depth--;
                        if("rows".equals(parser.getParsingContext().getParent().getCurrentName()) && next.has("doc") && ! next.get("doc").has("_attachments")) {
                            break;
                        } else {
                            next = null;
                        }
                    }
                }
            } catch(Exception e) {
                throw new RuntimeException(e);
            }
        }
    }


}
