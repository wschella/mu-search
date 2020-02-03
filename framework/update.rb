# This file contains information regarding updates retrieved through
# the delta service.

# TODO I'm not sure what this does.
def configure_properties_types_lookup_tables configuration
  rdf_properties = {}
  rdf_types = {}

  configuration["types"].each do |type_def|
    if type_def["composite_types"]
      type_def["composite_types"].each do |source_type|
        rdf_type = settings.type_definitions[source_type]["rdf_type"]
        rdf_types[rdf_type] = rdf_types[rdf_type] || []
        rdf_types[rdf_type].push source_type
      end

      type_def["properties"].each do |property|
        type_def["composite_types"].each do |source_type|
          property_name =
            if property["mappings"]
              property["mappings"][source_type] || property["name"]
            else
              property["name"]
            end

          rdf_property = settings.type_definitions[source_type][property_name]
          if property_name == "data"
            rdf_property = rdf_property["via"]
          end
          rdf_property = rdf_property.is_a?(Array) ? rdf_property : [rdf_property]
          rdf_property.each_index do |index|
            prop = rdf_property[index]
            rdf_properties[prop] =  rdf_properties[prop] || []
            rdf_properties[prop].push [type_def["type"], rdf_property.take(index)]
          end
        end
      end
    else
      rdf_types[type_def["rdf_type"]]  = rdf_types[type_def["rdf_type"]] || []
      rdf_types[type_def["rdf_type"]].push type_def["type"]
      
      type_def["properties"].each do |name, rdf_property|
        if name == "data"
          rdf_property = rdf_property["via"]
        end
        rdf_property = rdf_property.is_a?(Array) ? rdf_property : [rdf_property]
        rdf_property.each_index do |index|
          prop = rdf_property[index]
          rdf_properties[prop] =  rdf_properties[prop] || []
          rdf_properties[prop].push [type_def["type"], rdf_property.take(index)]
        end
      end
    end
  end

  return rdf_properties, rdf_types
end

# Parses the delta's as received from the delta service.
#
#   - raw_deltas: JSON parsed delta content.  Assumes to receive an
#     object with { delta: { inserts: [], deletes: [] } } which is
#     close to the 0.0.0-genesis format.
#
# Yields back contents in a format used internally.
#
# TODO: Consider using the current latest delta-notifier format.
def parse_deltas raw_deltas
  unless raw_deltas.empty?
    # Assumes there is only one application graph
    if raw_deltas.is_a?(Array)
      raw_deltas = raw_deltas.first
    end

    inserts = raw_deltas["delta"]["inserts"] || []
    inserts = inserts && inserts.map { |t| [:+, t["s"], t["p"], t["o"]] }

    deletes = raw_deltas["delta"]["deletes"] || []
    deletes = deletes && deletes.map { |t| [:-, t["s"], t["p"], t["o"]] }

    inserts + deletes
  end
end


# Update all documents relating to a particular uri and a series of
# types.
#
#   - client: ElasticSearch client in which updates need to occur.
#   - s: String URI of the entity which needs changing.
#   - types: Array of types for which the document needs updating.
def update_document_all_types client, s, types
  log.debug "Will update document #{s} for types #{types}"

  if types
    types.each do |type|
      log.debug "Updating document #{s} for type #{type}"
      indexes = Indexes.instance.get_indexes type
      indexes.each do |key, index|
        allowed_groups = index[:allowed_groups]
        log.debug "Got allowed groups for updated_document_all_types #{allowed_groups}"
        rdf_type = settings.type_definitions[type]["rdf_type"]
        log.debug "Got RDF type for updated_document_all_types #{rdf_type}"
        if is_authorized s, rdf_type, allowed_groups
          log.debug "Our current index knows that #{s} is of type #{rdf_type} based on allowed groups #{allowed_groups}"
          properties = settings.type_definitions[type]["properties"]
          document, attachment_pipeline =
                    fetch_document_to_index uri: s, properties: properties,
                                            allowed_groups: index[:allowed_groups]

          document_for_reporting = document.clone
          document_for_reporting["data"] = document_for_reporting["data"] ? document_for_reporting["data"].length : "none"

          log.debug "Fetched document to index is #{document_for_reporting}"

          # TODO what is uuid supposed to be here?  We abstract its meaning to be get_uuid(s) but are not sure
          uuid = get_uuid(s)

          if attachment_pipeline
            log.debug "Document to update has attachment pipeline"
            begin
              # client.upload_attachment index, uuid, attachment_pipeline, document
              client.upload_attachment index[:index], uuid, attachment_pipeline, document
              log.debug "Managed to upload attachment for #{s}"
            rescue
              log.warn "Could not upload attachment #{s}"
              begin
                log.debug "Trying to update document with id #{uuid}"
                client.update_document index[:index], uuid, document
                log.debug "Succeeded in updating document with id #{uuid}"
              rescue
                log.debug "Failed to update document, trying to put new document #{uuid}"
                client.put_document index[:index], uuid, document
                log.debug "Succeeded in putting new document #{uuid}"
              end
            end
          else
            begin
              log.debug "Trying to update document with id #{uuid}"
              client.update_document index[:index], uuid, document
              log.debug "Succeeded in updating document with id #{uuid}"
            rescue
              log.debug "Failed to update document, trying to put new document #{uuid}"
              client.put_document index[:index], uuid, document
              log.debug "Succeeded in putting new document #{uuid}"
            end
          end
        else
          log.info "Not Authorized."
        end
      end
    end
  end
end

# Deletes all decoments relating to a particular uri and a series of
# types.
#
#   - client: ElasticSearch client in which the updates need to occur.
#   - s: String URI of the entity which needs changing.
#   - types: Array of types for which the document needs updating.
def delete_document_all_types client, s, types
  log.debug "Will delete document #{s} for types #{types}"
  types.each do |type|
    # TODO: this fails because the uuid is already removed, should probably use uri as id in elastic?
    uuid = get_uuid s
    if uuid
      log.debug "deleting document #{s} with uuid #{uuid} for type #{type}"
      Settings.instance.indexes[type].each do |key, index|
        begin
          client.delete_document index[:index], uuid
        rescue
          log.info "Failed to delete document: #{uuid} in index: #{index[:index]}"
        end
      end
    end
  end
end
