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
          if attachment_pipeline
            begin
              # TODO what is uuid supposed to be here?
              # client.upload_attachment index, uuid, attachment_pipeline, document
              client.upload_attachment index, get_uuid(s), attachment_pipeline, document
            rescue
              log.warn "Could not upload attachment #{s}"
            end
          else
            begin
              client.update_document index[:index], get_uuid(s), document
            rescue
              client.put_document index[:index], get_uuid(s), document
            end
          end
        else
          log.info "Not Authorized."
        end
      end
    end
  end
end


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
