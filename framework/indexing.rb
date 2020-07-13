require 'parallel'
require 'concurrent'
# This file contains helpers for indexing documents.

# TODO: Describe this method.  I don't know what it means.
def is_multiple_type? type_definition
  type_definition["composite_types"].is_a?(Array)
end

# TODO: Describe this method.  I don't know what it means.
def multiple_type_expand_subtypes types, properties
  types.map do |type|
    source_type_def = settings.type_definitions[type]
    rdf_type = source_type_def["rdf_type"]

    {
      "type" => type,
      "rdf_type" => rdf_type,
      "properties" => Hash[
        properties.map do |property|
          property_name = property["name"]
          mapped_name =
            if property["mappings"]
              property["mappings"][type] || property_name
            else
              property_name
            end
          [property_name, source_type_def["properties"][mapped_name]]
        end
      ]
    }
  end
end

# Indexes documents in batches.  Ensuring the necessary indexes exist
# and are up-to-date.
#
#   - client: ElasticSearch client to execute the indexing on.
#   - type: Type of content which needs to be indexed
#   - index: Index to push the indexed documents in
#   - allowed_groups: Groups used for querying the database
#
# Documents are indexed in batches, thereby lowering the load on
# ElasticSearch.
#
def index_documents client, tika_client, type, index, allowed_groups = nil
  log.debug "Allowed groups in index #{allowed_groups}"
  type_def = settings.type_definitions[type]

  if is_multiple_type?(type_def)
    type_defs = multiple_type_expand_subtypes type_def["composite_types"], type_def["properties"]
  else
    type_defs = [type_def]
  end
  builder = MuSearch::IndexBuilder.new(elastic_client: client,
                                       number_of_threads: settings.number_of_threads,
                                       logger: log,
                                       index_definitions: type_defs,
                                       index_id: index,
                                       batch_size: settings.batch_size,
                                       max_batches: settings.max_batches,
                                       allowed_groups: allowed_groups,
                                       attachment_path_base: settings.attachment_path_base
                                      )
  builder.build
  #TODO: this used to return  { index: index, document_types: count_list }
end
