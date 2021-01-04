require 'net/http'
require 'digest'
require 'set'
require 'request_store'
require 'listen'
require 'singleton'
require 'base64'
require 'open3'
require 'webrick'

require_relative 'lib/logger.rb'
require_relative 'lib/mu_search/sparql.rb'
require_relative 'lib/mu_search/authorization_utils.rb'
require_relative 'lib/mu_search/delta_handler.rb'
require_relative 'lib/mu_search/automatic_update_handler.rb'
require_relative 'lib/mu_search/invalidating_update_handler.rb'
require_relative 'lib/mu_search/config_parser.rb'
require_relative 'lib/mu_search/document_builder.rb'
require_relative 'lib/mu_search/index_builder.rb'
require_relative 'lib/mu_search/search_index.rb'
require_relative 'lib/mu_search/index_manager.rb'
require_relative 'framework/elastic.rb'
require_relative 'framework/tika.rb'
require_relative 'framework/search.rb'

##
# WEBrick setup
##
max_uri_length = ENV["MAX_REQUEST_URI_LENGTH"].to_i > 0 ? ENV["MAX_REQUEST_URI_LENGTH"].to_i : 10240
log.info "Set WEBrick MAX_URI_LENGTH to #{max_uri_length}"
WEBrick::HTTPRequest.const_set("MAX_URI_LENGTH", max_uri_length)
max_header_length = ENV["MAX_REQUEST_HEADER_LENGTH"].to_i > 0 ? ENV["MAX_REQUEST_HEADER_LENGTH"].to_i : 1024000
log.info "Set WEBrick MAX_HEADER_LENGTH to #{max_header_length}"
WEBrick::HTTPRequest.const_set("MAX_HEADER_LENGTH", max_header_length)

SinatraTemplate::Utils.log.formatter = proc do |severity, datetime, progname, msg|
  "#{severity} [\##{$$}] #{progname} -- #{msg}\n"
end

before do
  request.path_info.chomp!('/')
  content_type 'application/vnd.api+json'
end

##
# Setup index manager based on configuration
##
def setup_index_manager elasticsearch, tika, config
  search_configuration = config.select do |key|
    [:type_definitions, :default_index_settings, :additive_indexes,
     :persist_indexes, :eager_indexing_groups, :number_of_threads,
     :batch_size, :max_batches, :attachment_path_base].include? key
  end
  MuSearch::IndexManager.new(
    logger: SinatraTemplate::Utils.log,
    elasticsearch: elasticsearch,
    tika: tika,
    search_configuration: search_configuration)
end

##
# Setup delta handling based on configuration
##
def setup_delta_handling(index_manager, elasticsearch, tika, config)
  if config[:automatic_index_updates]
    search_configuration = config.select do |key|
      [:type_definitions, :number_of_threads, :update_wait_interval_minutes, :attachment_path_base].include? key
    end
    handler = MuSearch::AutomaticUpdateHandler.new(
      logger: SinatraTemplate::Utils.log,
      index_manager: index_manager,
      elasticsearch: elasticsearch,
      tika: tika,
      search_configuration: search_configuration)
  else
    search_configuration = config.select do |key|
      [:type_definitions, :number_of_threads, :update_wait_interval_minutes].include? key
    end
    handler = MuSearch::InvalidatingUpdateHandler.new(
      logger: SinatraTemplate::Utils.log,
      index_manager: index_manager,
      search_configuration: search_configuration)
  end

  delta_handler = MuSearch::DeltaHandler.new(
    update_handler: handler,
    logger: SinatraTemplate::Utils.log,
    search_configuration: { type_definitions: config[:type_definitions] } )
  delta_handler
end

##
# Configures the system and makes sure everything is up.
##
configure do
  set :protection, :except => [:json_csrf]
  set :dev, (ENV['RACK_ENV'] == 'development')

  configuration = MuSearch::ConfigParser.parse('/config/config.json')
  set configuration

  tika = Tika.new(host: 'tika', port: 9998, logger: SinatraTemplate::Utils.log)
  elasticsearch = Elastic.new(host: 'elasticsearch', port: 9200, logger: SinatraTemplate::Utils.log)

  while !elasticsearch.up
    log.info "...waiting for elasticsearch..."
    sleep 1
  end

  while !MuSearch::SPARQL.up? do
    log.info "...waiting for SPARQL endpoint..."
    sleep 1
  end

  index_manager = setup_index_manager elasticsearch, tika, configuration
  set :index_manager, index_manager
  delta_handler = setup_delta_handling index_manager, elasticsearch, tika, configuration
  set :delta_handler, delta_handler
end

###
# API ENDPOINTS
###

# Processes an update from the delta system.
# See MuSearch::DeltaHandler and MuSearch::UpdateHandler for more info
post "/update" do
  settings.delta_handler.handle_deltas @json_body
  { message: "Thanks for all the updates." }.to_json
end


# Updates the indexes for the given :path.
# If an authorization header is provided, only the authorized
# indexes are updated.
# Otherwise, all indexes for the path are updated.
#
# Use _all as path to update all index types
#
# Note:
# - the search index is only marked as invalid in memory.
#   The index is not removed from Elasticsearch nor the triplestore.
#   Hence, on restart of mu-search, the index will be considered valid again.
# - an invalidated index will be updated before executing a search query on it.
post "/:path/index" do |path|
  allowed_groups = get_allowed_groups
  log.debug("AUTHORIZATION") { "Index update request received allowed groups: #{allowed_groups || 'none'}" }

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.fetch_indexes index_type, allowed_groups, force_update: true

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Invalidates the indexes for the given :path.
# If an authorization header is provided, only the authorized
# indexes are invalidated.
# Otherwise, all indexes for the path are invalidated.
#
# Use _all as path to invalidate all index types
#
# Note:
# - the search index is only marked as invalid in memory.
#   The index is not removed from Elasticsearch nor the triplestore.
#   Hence, on restart of mu-search, the index will be considered valid again.
# - an invalidated index will be updated before executing a search query on it.
post "/:path/invalidate" do |path|
  allowed_groups = get_allowed_groups
  log.debug("AUTHORIZATION") { "Index invalidation request received allowed groups: #{allowed_groups || 'none'}" }

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.invalidate_indexes index_type, allowed_groups

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Removes the indexes for the given :path.
# If an authorization header is provided, only the authorized
# indexes are removed.
# Otherwise, all indexes for the path are removed.
#
# Use _all as path to remove all index types
#
# Note: a removed index will be recreated before executing a search query on it.
delete "/:path" do |path|
  allowed_groups = get_allowed_groups
  log.debug("AUTHORIZATION") { "Index delete request received allowed groups: #{allowed_groups || 'none'}" }

  index_type = path == "_all" ? nil : path
  index_manager = settings.index_manager
  indexes = index_manager.remove_indexes index_type, allowed_groups

  data = indexes.map do |index|
    {
      type: "indexes",
      id: index.name,
      attributes: {
        uri: index.uri,
        status: index.status,
        'allowed-groups' => index.allowed_groups
      }
    }
  end

  { data: data }.to_json
end

# Performs a regular search.
#
# Creates new indexes, updates older ones and keeps constructed ones
# as necessary.  This is the standard entrypoint for your mu-search
# queries.
#
# Check Readme.md for more information.
#
# TODO move this method up as it's the most common entrypoint
#
# TODO fleshen out functionality with respect to existing indexes
get "/:path/search" do |path|
  allowed_groups = get_allowed_groups_with_fallback
  log.debug("AUTHORIZATION") { "Search request received allowed groups #{allowed_groups}" }

  elasticsearch = Elastic.new(host: 'elasticsearch', port: 9200)

  index_manager = settings.index_manager
  type_def = settings.type_paths[path]
  type_name = type_def and type_def["type"]

  collapse_uuids = (params["collapse_uuids"] == "t")

  if params["page"]
    page = (params["page"]["number"] && params["page"]["number"].to_i) || 0
    size = (params["page"]["size"] && params["page"]["size"].to_i) || 10
  else
    page = 0
    size = 10
  end

  indexes = index_manager.fetch_indexes type_name, allowed_groups

  if indexes.length == 0
    log.info("SEARCH") { "No indexes found to search in. Returning empty result" }
    format_results(type_def, 0, page, size, []).to_json
  else
    # TODO << start move to ES query utils
    es_query = construct_es_query type_def

    log.debug "[Search] ElasticSearch query: #{es_query}"
    log.debug "[Search] ElasticSearch query as json: #{es_query.to_json}"

    count_query = es_query.clone

    sort_statement = es_query_sort_statement

    if sort_statement
      es_query['sort'] = sort_statement
    end

    es_query["from"] = page * size
    es_query["size"] = size

    if collapse_uuids
      es_query["collapse"] = { field: "uuid" }
      es_query["aggs"] = { "type_count" => { "cardinality" => { "field" => "uuid" } } }
    end

    # Exclude all attachment fields for now
    attachments = settings.type_definitions[type_def]["properties"].select {|key, val|
      val.is_a?(Hash) && val["attachment_pipeline"]
    }
    es_query["_source"] = {
      excludes: attachments.keys
    }
    # TODO end move to ES query utils >>

    index_names = indexes.map { |index| index.name }
    index_string = index_names.join(',')

    while indexes.any? { |index| index.status == :updating }
      log.info("SEARCH") { "Waiting for indexes to be up-to-date..." }
      sleep 0.5
    end

    log.debug("SEARCH") { "All indexes are up to date" }
    log.debug "[Elasticsearch] Running ES query: #{es_query.to_json}"

    response = elasticsearch.search(index: index_string, query: es_query)
    if response.kind_of?(String) # assume success
      results = JSON.parse(response)

      count =
        if collapse_uuids
          results["aggregations"]["type_count"]["value"]
        else
          count_result = JSON.parse(elasticsearch.count index: index_string, query: count_query)
          count_result["count"]
        end

      log.debug("SEARCH") { "Found #{count} results" }
      format_results(type_def, count, page, size, results).to_json
    else
      log.warn("SEARCH") { "Execution of search query failed: #{response}" }
      log.debug("SEARCH") { response.body }
      error(response.message)
    end
  end
end


# Raw ES Query DSL
# Need to think through several things, such as pagination
if settings.enable_raw_dsl_endpoint
  post "/:path/search" do |path|
    client = Elastic.new(host: 'elasticsearch', port: 9200)
    type = settings.type_paths[path]
    index_names = get_or_create_indexes client, type

    return [].to_json if index_names.length == 0

    index_string = index_names.join(',')

    es_query = @json_body

    count_query = es_query.clone
    count_query.delete("from")
    count_query.delete("size")
    count_result = JSON.parse(client.count index: index_string, query: es_query)
    count = count_result["count"]

    format_results(type, count, 0, 10, client.search(index: index_string, query: es_query)).to_json
  end
end

# Health report
# TODO Make this more descriptive - status of all indexes?
get "/health" do
  { status: "up" }.to_json
end
