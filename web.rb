require 'net/http'
require 'digest'
require 'set'
require 'request_store'

require_relative 'framework/elastic.rb'
require_relative 'framework/sparql.rb'
require_relative 'framework/authorization.rb'
require_relative 'framework/indexing.rb'
require_relative 'framework/search.rb'
require_relative 'framework/updates.rb'

configure do
  configuration = JSON.parse File.read('/config/config.json')
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  set :db, SinatraTemplate::SPARQL::Client.new('http://db:8890/sparql', {})
  
  set :master_mutex, Mutex.new

  set :mutex, {}

  set :batch_size, (ENV['BATCH_SIZE'] || configuration["batch_size"] || 100)

  set :automatic_index_updates,  
      (ENV["AUTOMATIC_INDEX_UPDATES"] || configuration["automatic_index_updates"])

  set :type_paths, Hash[
        configuration["types"].collect do |type_def|
          [type_def["on_path"], type_def["type"]]
        end
      ]

  set :type_definitions, Hash[
        configuration["types"].collect do |type_def|
          [type_def["type"], type_def]
        end
      ]

  indexes = {}
  configuration["types"].each do |type_def|
    type = type_def["type"]
    indexes[type] = load_indexes type
  end

  set :indexes, indexes

  set :index_status, {}

  # properties and types lookup tables for deltas
  # { <rdf_property> => [type, ...] }
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

  set :rdf_properties, rdf_properties
  set :rdf_types, rdf_types

  eager_indexing_groups = configuration["eager_indexing_groups"] || []
  
  # if configuration["eager_indexing_sparql_query"]
  #   query_result = query configuration["eager_indexing_sparql_query"]
  #   eager_indexing_groups += query_result.map { |key, value| value.to_s }
  # end

  unless eager_indexing_groups.empty?
    while !sparql_up
      sleep 0.5
    end

    settings.master_mutex.synchronize do
      eager_indexing_groups.each do |groups|
        settings.type_definitions.keys.each do |type|
          index = find_matching_index type, groups, groups
          index_documents client, type, index, groups
        end
      end
    end
  end
end


def get_type_from_path path
  settings.type_paths[path]
end


get "/:path/index" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path

  def sync client, type
    settings.master_mutex.synchronize do
      index = get_request_index type

      unless index
        index = create_request_index client, type
      end

      settings.index_status[index] = :updating
      return index
    end
  end

  index = sync client, type

  settings.mutex[index].synchronize do
    report = index_documents client, type, index
    settings.index_status[index] = :valid
    return report
  end
end


get "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path

  index = get_index_safe client, type

  es_query = construct_es_query
  count_query = es_query.clone

  if params["page"]
    page = (params["page"]["number"] && params["page"]["number"].to_i) || 0
    size = (params["page"]["size"] && params["page"]["size"].to_i) || 10
  else
    page = 0
    size = 10
  end

  es_query["from"] = page * size
  es_query["size"] = size

  while settings.index_status[index] == :updating
    sleep 0.5
  end
  count_result = JSON.parse(client.count index: index, query: count_query)
  count = count_result["count"]

  results = client.search index: index, query: es_query

  format_results(type, count, page, size, results).to_json
end


# Using raw ES search DSL, mostly for testing
# Need to think through several things, such as pagination
post "/:path/search" do |path|
  content_type 'application/json'
  client = Elastic.new(host: 'elasticsearch', port: 9200)
  type = get_type_from_path path

  index = get_index_safe client, type

  es_query = @json_body

  count_query = es_query
  count_query.delete("from")
  count_query.delete("size")
  count_result = JSON.parse(client.count index: index, query: es_query)
  count = count_result["count"]

  format_results(type, count, 0, 10, client.search(index: index, query: es_query)).to_json
end


post "/update" do
  client = Elastic.new(host: 'elasticsearch', port: 9200)

  # [[d, s, p, o], ...] where d is :- or :+ for deletes/inserts respectively
  deltas = parse_deltas @json_body

  # Tabulate first, to avoid duplicate updates
  # { <uri> => [type, ...] } or { <uri> => false } if document should be deleted
  # should be inverted to { <type> => [uri, ...] } for easier index-specific blocking
    

  if settings.automatic_index_updates
    docs_to_update, docs_to_delete = tabulate_updates deltas
    docs_to_update.each { |s, types| update_document_all_types client, s, types }
    docs_to_delete.each { |s, types| delete_document_all_types client, s, types }
  else
    invalidate_updates deltas
  end



  { message: "Thanks for all the updates." }.to_json
end


post "/settings/automatic_updates" do
  settings.automatic_index_updates = true
end 


delete "/settings/automatic_updates" do
  settings.automatic_index_updates = false
end 
