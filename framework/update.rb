# This file contains information regarding updates retrieved through
# the delta service.

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
