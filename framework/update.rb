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
          rdf_property = rdf_property.is_a?(Array) ? rdf_property : [rdf_property]
          rdf_property.each do |prop|
            rdf_properties[prop] =  rdf_properties[prop] || []
            rdf_properties[prop].push type_def["type"]
          end
        end
      end
    else
      rdf_types[type_def["rdf_type"]]  = rdf_types[type_def["rdf_type"]] || []
      rdf_types[type_def["rdf_type"]].push type_def["type"]
      type_def["properties"].each do |name, rdf_property|
        rdf_property = rdf_property.is_a?(Array) ? rdf_property : [rdf_property]
        rdf_property.each do |prop|
          rdf_properties[prop] =  rdf_properties[prop] || []
          rdf_properties[prop].push type_def["type"]
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

# Invalidates the updates as received by the parsed delta's.
#
#   - deltas: Delta's as converted by #parse_deltas.
#
# TODO: Update the specific documents rather than invalidating the
# full index.  It seems index invalidation makes subsequent queries
# take a substantial amount of time (that, or something went wrong).
#
# TODO: Find a way to capture changes occurring in in-between objects
# which may be found when subject paths are supplied in the
# configuration.
def invalidate_updates deltas
  deltas.each do |triple|
    delta, s, p, o = triple

    if p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      settings.rdf_types[o].each { |type| invalidate_indexes s, type }
    else
      possible_types = settings.rdf_properties[p]
      if possible_types
        possible_types.each do |type|
          rdf_type = settings.type_definitions[type]["rdf_type"]

          if is_type s, rdf_type
            invalidate_indexes s, type
          end
        end
      end
    end
  end
end

# Consumes parsed delta's and acts on instance types in order to
# figure out which documents to update and which documents to remove.
# Assumes that it is safe to remove objects for which the type was
# removed and that it needs to update the documents for inserts of the
# type.
#
#   - deltas: Delta's as parsed by #parse_deltas
#
# TODO: Consider coping with intermediate objects as introduced by
# array arrays in the subject configuration.
def tabulate_updates deltas
  docs_to_update = {}
  docs_to_delete = {}

  deltas.each do |triple|
    delta, s, p, o = triple

    if p == "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
      types = settings.rdf_types[o]
      if types
        types.each do |type|
          if delta == :-
            docs_to_update[s] = false
            triple_types = docs_to_delete[s] || Set[]
            docs_to_delete[s] = triple_types.add(type)
          else
            triple_types = docs_to_update[s] || Set[]
            docs_to_update[s] = triple_types.add(type)
          end
        end
      end
    else
      possible_types = settings.rdf_properties[p]
      if possible_types
        possible_types.each do |type|
          rdf_type = settings.type_definitions[type]["rdf_type"]

          if is_type s, rdf_type
            unless docs_to_update[s] == false
              triple_types = docs_to_update[s] || Set[]
              docs_to_update[s] = triple_types.add(type)
            end
          end
        end
      end
    end
  end

  return docs_to_update, docs_to_delete
end

# Update all documents relating to a particular uri and a series of
# types.
#
#   - client: ElasticSearch client in which updates need to occur.
#   - s: String URI of the entity which needs changing.
#   - types: Array of types for which the document needs updating.
def update_document_all_types client, s, types
  if types
    types.each do |type|
      indexes = Indexes.instance.get_indexes type
      indexes.each do |key, index|
        allowed_groups = index[:allowed_groups]
        rdf_type = settings.type_definitions[type]["rdf_type"]
        if is_authorized s, rdf_type, allowed_groups
          properties = settings.type_definitions[type]["properties"]
          document, attachment_pipeline =
            fetch_document_to_index uri: s, properties: properties,
                                    allowed_groups: index[:allowed_groups]
          # TODO what is uuid supposed to be here?  We abstract its meaning to be get_uuid(s) but are not sure
          uuid = get_uuid(s)

          begin
            log.info "Trying to update document with id #{uuid}"
            client.update_document index[:index], uuid, document
            log.info "Succeeded in updating document with id #{uuid}"
          rescue
            log.info "Failed to update document, trying to put new document #{uuid}"
            client.put_document index[:index], uuid, document
            log.info "Succeeded in putting new document #{uuid}"
          end

          if attachment_pipeline
            begin
              # client.upload_attachment index, uuid, attachment_pipeline, document
              client.upload_attachment index, uuid, attachment_pipeline, document
            rescue
              log.warn "Could not upload attachment #{s}"
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
  types.each do |type|
    uuid = get_uuid s
    if uuid
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
