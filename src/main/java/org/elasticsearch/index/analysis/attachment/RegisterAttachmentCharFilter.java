package org.elasticsearch.index.analysis.attachment;

import java.io.Reader;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.PreBuiltCharFilterFactoryFactory;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.indices.analysis.IndicesAnalysisService;

/**
 *
 */
public class RegisterAttachmentCharFilter extends AbstractIndexComponent {
    @Inject
    public RegisterAttachmentCharFilter(Index index, @IndexSettings Settings indexSettings, IndicesAnalysisService indicesAnalysisService) {
        super(index, indexSettings);

        indicesAnalysisService.charFilterFactories().put("attachments_test",
                new PreBuiltCharFilterFactoryFactory(new CharFilterFactory() {
                    @Override
                    public String name() {
                        return "attachments_test";
                    }

                    @Override
                    public Reader create(Reader reader) {
                    	return new AttachmentCharFilter(reader);
                    }
                }));
    }
}