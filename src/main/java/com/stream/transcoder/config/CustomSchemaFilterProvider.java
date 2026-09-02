package com.stream.transcoder.config;

import org.hibernate.boot.model.relational.Namespace;
import org.hibernate.boot.model.relational.Sequence;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.spi.SchemaFilter;
import org.hibernate.tool.schema.spi.SchemaFilterProvider;

public class CustomSchemaFilterProvider implements SchemaFilterProvider {

    @Override
    public SchemaFilter getCreateFilter() {
        return Filter.INSTANCE;
    }

    @Override
    public SchemaFilter getDropFilter() {
        return Filter.INSTANCE;
    }

    @Override
    public SchemaFilter getMigrateFilter() {
        return Filter.INSTANCE;
    }

    @Override
    public SchemaFilter getValidateFilter() {
        return Filter.INSTANCE;
    }

    @Override
    public SchemaFilter getTruncatorFilter() {
        return Filter.INSTANCE;
    }

    private static class Filter implements SchemaFilter {
        public static final Filter INSTANCE = new Filter();

        @Override
        public boolean includeNamespace(Namespace namespace) {
            return true;
        }

        @Override
        public boolean includeTable(Table table) {
            // Seule la table video_jobs sera créée/mise à jour par Hibernate DDL
            return "video_jobs".equalsIgnoreCase(table.getName());
        }

        @Override
        public boolean includeSequence(Sequence sequence) {
            return true;
        }
    }
}
