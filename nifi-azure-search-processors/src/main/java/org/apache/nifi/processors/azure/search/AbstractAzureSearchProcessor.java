package org.apache.nifi.processors.azure.search;

import com.azure.search.documents.SearchClient;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import org.apache.nifi.azure.search.AzureSearchConnectionService;

import java.util.List;

public abstract class AbstractAzureSearchProcessor extends AbstractProcessor {
    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Example success relationship")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Example success relationship")
            .build();

    static final PropertyDescriptor CONNECTION_SERVICE = new PropertyDescriptor.Builder()
            .name("azure-search-connection-service")
            .displayName("Azure Search Connection Service")
            .description("If configured, the controller service used to obtain the connection string and access key")
            .required(false)
            .identifiesControllerService(AzureSearchConnectionService.class)
            .build();

    static final List<PropertyDescriptor> descriptors = List.of(
            CONNECTION_SERVICE
    );

    private SearchClient searchClient;
    private AzureSearchConnectionService connectionService;

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final ComponentLog logger = getLogger();
        if (context.getProperty(CONNECTION_SERVICE).isSet()) {
            this.connectionService = context.getProperty(CONNECTION_SERVICE).asControllerService(AzureSearchConnectionService.class);
            this.searchClient = this.connectionService.getSearchClient();
        }
    }

    @OnStopped
    public final void onStopped() {
        final ComponentLog logger = getLogger();
        if (connectionService == null && searchClient != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Closing search client");
            }
            this.searchClient = null;
        }
    }

    protected String getURI(final ProcessContext context) {
        return this.connectionService.getURI();
    }

    protected String getAccessKey(final ProcessContext context) {
        return this.connectionService.getAccessKey();
    }

    protected String getIndexName(final ProcessContext context) {
        return this.connectionService.getIndexName();
    }

    protected SearchClient getSearchClient() {
        return searchClient;
    }
}
