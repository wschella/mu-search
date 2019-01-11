def parse_deltas raw_deltas
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


def invalidate_indexes s, type
  Settings.instance.indexes[type].each do |key, index| 
    allowed_groups = index[:allowed_groups]
    rdf_type = settings.type_definitions[type]["rdf_type"]

    if is_authorized s, rdf_type, allowed_groups
      settings.mutex[index[:index]].synchronize do
        settings.index_status[index[:index]] = :invalid 
      end
    else
      log.info "Not Authorized, nothing doing."
    end
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
      indexes = Settings.instance.indexes[type]
      indexes.each do |key, index|
        allowed_groups = index[:allowed_groups]
        rdf_type = settings.type_definitions[type]["rdf_type"]
        if is_authorized s, rdf_type, allowed_groups
          properties = settings.type_definitions[type]["properties"]
          document =
            fetch_document_to_index uri: s, properties: properties, 
                                    allowed_groups: index[:allowed_groups]
          begin
            client.update_document index[:index], get_uuid(s), document
          rescue
            client.put_document index[:index], get_uuid(s), document
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
    log.info "Deleting #{s}"
    uuid = get_uuid(s)
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
