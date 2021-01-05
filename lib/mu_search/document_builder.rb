module MuSearch
  class DocumentBuilder
    def initialize( tika:, sparql_client:, attachment_path_base:, logger: )
      @tika = tika
      @sparql_client = sparql_client
      @attachment_path_base = attachment_path_base
      @cache_path_base = "/cache/"
      @logger = logger
    end

    # Constructs a document to index for the given resource URI and configured properties.
    #
    # The properties are queried from the triplestore using the DocumentBuilder's SPARQL client
    # which is configured with the appropriate mu-auth-allowed-groupds.
    #
    # This is your one-stop shop to fetch all info to index a document.
    #   - uri: URI of the resource to fetch
    #   - properties: Array of properties as configured in the search config
    def fetch_document_to_index( uri: nil, properties: nil )
      # we include uuid because it may be used for folding
      properties["uuid"] = ["http://mu.semte.ch/vocabularies/core/uuid"] unless properties.has_key?("uuid")

      key_value_tuples = properties.collect do |key, prop_config|
        query = make_property_query(uri, key, prop_config)
        results = @sparql_client.query(query)
        prop_values = results.collect { |result| result[key] }

        index_value = []
        if prop_config.is_a? Hash
          if prop_config["attachment_pipeline"] # file field
            index_value = build_file_field(prop_values)
          elsif prop_config["rdf_type"] # nested object
            index_value = build_nested_object(prop_values, prop_config["properties"])
          else
            raise "Invalid configuration for property #{key}. If the property configuration is a hash, it must either be a file field or nested object configuration."
          end
        else
          index_value = build_simple_property(prop_values)
        end

        [key, denumerate(index_value)]
      end

      Hash[key_value_tuples]
    end


    private

    def log
      @logger
    end

    # Constructs a SPARQL query which selects the given property of a resource
    # and returns it bound to the given property key
    #   - uri: URI of the resource as a string
    #   - property_key: name of the variable to bind the result to
    #   - property_predicate: predicate (path) of the property to fetch
    def make_property_query( uri, property_key, property_predicate )
      predicate = property_predicate.is_a?(Hash) ? property_predicate["via"] : property_predicate
      predicate_s = MuSearch::SPARQL.make_predicate_string predicate
      <<SPARQL
    SELECT DISTINCT ?#{property_key} WHERE {
      #{sparql_escape_uri(uri)} #{predicate_s} ?#{property_key}
    }
SPARQL
    end

    # Get the array of values to index for a given SPARQL result set of simple values.
    # Values are constructed based on the literal datatype.
    def build_simple_property( values )
      values.collect do |value|
        case value
        when RDF::Literal::Integer
          value.to_i
        when RDF::Literal::Double
          value.to_f
        when RDF::Literal::Decimal
          value.to_f
        when RDF::Literal::Boolean
          value.to_s.downcase == "true"
        when RDF::Literal::Time
          value.to_s
        when RDF::Literal::Date
          value.to_s
        when RDF::Literal::DateTime
          value.to_s
        when RDF::Literal
          value.to_s
        else
          value.to_s
        end
      end
    end

    # Get the array of objects to be indexed for a given SPARQL result set
    # of related resources configured to be indexed as nested object.
    # The properties to be indexed for the nested object are passed as an argument.
    def build_nested_object( related_resources, nested_properties )
      related_resources.collect do |resource_uri|
        nested_document = fetch_document_to_index(uri: resource_uri, properties: nested_properties)
        nested_document
      end
    end

    # Get the array of file objects to be indexed for a given set of file URIs.
    #
    # The file object to index currently contains the following properties:
    # - content: text content of the file
    # This list may be extended with additional metadata in the future.
    def build_file_field( file_uris )
      file_uris.collect do |file_uri|
        file_path = File.join(@attachment_path_base, file_uri.to_s.sub("share://", ""))
        file_size = File.size(file_path)
        if file_size < ENV["MAXIMUM_FILE_SIZE"].to_i
          content = extract_text_content(file_path)
        else
          @logger.warn("INDEXING") { "File #{file_path} (#{filesize} bytes) exceeds the allowed size of #{ENV["MAXIMUM_FILE_SIZE"]} bytes. File content will not be indexed." }
          content = nil
        end

        { content: content }
      end
    end

    # Extract the text content of the file at the given path using Tika.
    # Use a previously cached result if one is available.
    # On successfull processing, returns the extracted text content.
    # Otherwise, returns nil.
    #
    # Entries are cached using the file hash as key.
    def extract_text_content( file_path )
      begin
        file = File.open(file_path, "rb")
        blob = file.read
        file.close
        file_hash = Digest::SHA256.hexdigest blob
        cached_file_path = "#{@cache_path_base}#{file_hash}"
        if File.exists? cached_file_path
          text_content = File.open(cached_file_path, mode: "rb", encoding: 'utf-8') do |file|
            @logger.debug("TIKA") { "Using cached result #{cached_file_path} for file #{file_path}" }
            file.read
          end
        else
          text_content = @tika.extract_text file_path, blob
          if text_content.nil?
            @logger.info("TIKA") { "Received empty result from Tika for file #{file_path}. File content will not be indexed." }
            # write emtpy file to make cache hit on next run
            File.open(cached_file_path, "w") {}
          else
            @logger.debug("TIKA") { "Extracting text from #{file_path} and storing result in #{cached_file_path}" }
            File.open(cached_file_path, "w") do |file|
              file.puts text_content.force_encoding("utf-8").unicode_normalize
            end
          end
        end
        text_content
      rescue Errno::ENOENT, IOError => e
        @logger.warn("TIKA") { "Error reading file at #{file_path} to extract content. File content will not be indexed." }
        @logger.warn("TIKA") { e.full_message }
        nil
      rescue StandardError => e
        @logger.warn("TIKA") { "Failed to extract content of file #{file_path}. File content will not be indexed." }
        @logger.warn("TIKA") { e.full_message }
        nil
      end
    end


    # Utility function to denumerate the given array value.
    # I.e.
    # - returns nil if the given array is empty
    # - returns a single value if the given array only contains one element
    # - returns the array value if the given array contains mulitple elements
    def denumerate( value )
      case value.length
      when 0 then nil
      when 1 then value.first
      else value
      end
    end
  end
end
