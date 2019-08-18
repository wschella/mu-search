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
# ElasticSearch.  Our experiments show that a multi-core system barely
# receives load in this setup.  We suppose this can be solved by
# running these requests in parallel but that's not clear.
#
# TODO: Optimize this code so indexing can use more than 8 cores.
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

    (0..batches).each do |i|
      batch_start_time = Time.now
      log.info "Indexing batch #{i} of #{count/settings.batch_size}"
      offset = i*settings.batch_size
      data = Concurrent::Array.new
      q = <<SPARQL
    SELECT DISTINCT ?id WHERE {
      ?doc a <#{rdf_type}>;
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
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

       Parallel.each( query_result, in_threads: 32 ) do |result|
          uuid = result[:id].to_s

          log.debug "Fetching document for uuid #{uuid}"

          begin
            document, attachment_pipeline = fetch_document_to_index uuid: uuid, properties: properties, allowed_groups: allowed_groups

            document["uuid"] = uuid

            log.debug "Uploading document #{uuid} - batch #{i} - allowed groups #{allowed_groups}"

            if attachment_pipeline
              data.push({ index: { _id: uuid , pipeline: "attachment" } }, document)
            else
              data.push({ index: { _id: uuid } }, document)
            end
          rescue StandardError => e
            log.warn "Failed to fetch document or upload it or somesuch.  ID #{uuid} error #{e.inspect}"
          end
        # end
      end
      # Process.waitall
      begin
        log.info "Bulk updating documents for batch #{i} - index #{index} - data #{data.length}"
        client.bulk_update_document index, data unless data.empty?
        log.info "Bulk updated documents for batch #{i}"
      rescue StandardError => e
        log.warn e
        ids_as_string =
          data
            .select { |d| d[:index] && d[:index][:_id] }
            .map { |d| d[:index][:_id] }
            .join( "," )
        log.warn "Failed to ingest batch for ids #{ids_as_string}"
      end

      log.info "Processed batch in #{(Time.now - batch_start_time).round} seconds"
    end
  end

  { index: index, document_types: count_list }
end
