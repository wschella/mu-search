def is_multiple_type? type_definition
  type_definition["composite_types"].is_a?(Array)
end


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




def index_documents client, type, index, allowed_groups = nil
  count_list = [] # for reporting

  type_def = settings.type_definitions[type]

  if is_multiple_type?(type_def)
    type_defs = multiple_type_expand_subtypes type_def["composite_types"], type_def["properties"]
  else
    type_defs = [type_def]
  end

  type_defs.each do |type_def|
    rdf_type = type_def["rdf_type"]

    count = count_documents rdf_type
    type_def["count"] = count
    properties = type_def["properties"]

    (0..(count/settings.batch_size)).each do |i|
      offset = i*settings.batch_size
      data = []
      q = <<SPARQL
    SELECT DISTINCT ?id WHERE {
      ?doc a <#{rdf_type}>;
           <http://mu.semte.ch/vocabularies/core/uuid> ?id
    } LIMIT 100 OFFSET #{offset}
SPARQL

    query_result =
      if allowed_groups
        authorized_query q, allowed_groups
      else
        request_authorized_query q
      end

      query_result.each do |result|
        uuid = result[:id].to_s
        document = fetch_document_to_index uuid: uuid, properties: properties
        data.push({ index: { _id: uuid } })
        data.push document
      end

      client.bulk_update_document index, data unless data.empty?

    end
  end

  { index: index, document_types: type_defs }.to_json
end

