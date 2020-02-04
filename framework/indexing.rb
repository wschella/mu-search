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
def index_documents client, type, index, allowed_groups = nil
  log.debug "Allowed groups in index #{allowed_groups}"

  count_list = [] # for reporting

  type_def = settings.type_definitions[type]

  if is_multiple_type?(type_def)
    type_defs = multiple_type_expand_subtypes type_def["composite_types"], type_def["properties"]
  else
    type_defs = [type_def]
  end

  type_defs.each do |type_def|
    rdf_type = type_def["rdf_type"]
    count = count_documents rdf_type, allowed_groups
    count_list.push({type: type_def["type"], count: count})
    properties = type_def["properties"]

    log.info "Indexing #{count} documents of type: #{type_def["type"]}"

    batches =
      if settings.max_batches and settings.max_batches != 0
        [settings.max_batches, count/settings.batch_size].min
      else
        count/settings.batch_size
      end

    log.info "Number of batches: #{batches}"

    Parallel.each( 0..batches, in_threads: 8 ) do |i|
      # TODO: make this thread number configurable, was (0..batches).each do |i|
      batch_start_time = Time.now
      log.info "Indexing batch #{i} of #{count/settings.batch_size}"
      offset = i*settings.batch_size

      q = <<SPARQL
    SELECT DISTINCT ?doc ?id WHERE {
      ?doc a <#{rdf_type}>.
    } LIMIT #{settings.batch_size} OFFSET #{offset}
SPARQL

      log.debug "selecting documents for batch #{i}"

      query_result =
        if allowed_groups
          authorized_query q, allowed_groups
        else
          request_authorized_query q
        end

      log.debug "Discovered identifiers for this batch: #{query_result}"

      number_of_threads = settings.batch_size > ENV['NUMBER_OF_THREADS'].to_i ? ENV['NUMBER_OF_THREADS'].to_i: settings.batch_size
      Parallel.each( query_result, in_threads: number_of_threads ) do |result|
        data = []
        document_id = result[:doc].to_s
        log.debug "Fetching document for #{document_id}"
        begin
          document, attachment_pipeline = fetch_document_to_index uri: document_id, properties: properties, allowed_groups: allowed_groups
          log.debug "Uploading document #{document_id} - batch #{i} - allowed groups #{allowed_groups}"
          if attachment_pipeline
            data.push({ index: { _id: document_id , pipeline: attachment_pipeline } }, document)
          else
            data.push({ index: { _id: document_id } }, document)
          end

          begin
            client.bulk_update_document index, data unless data.empty?
          rescue StandardError => e
            log.warn e
            ids_as_string =
              data
                .select { |d| d[:index] && d[:index][:_id] }
                .map { |d| d[:index][:_id] }
                .join( "," )
            log.warn "Failed to ingest batch for ids #{ids_as_string}"
          end
        rescue StandardError => e
          log.warn "Failed to fetch document or upload it or somesuch.  ID #{document_id} error #{e.inspect}"
        end
      end

      log.info "Processed batch in #{(Time.now - batch_start_time).round} seconds"
    end
  end

  { index: index, document_types: count_list }
end
