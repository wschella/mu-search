module MuSearch
  class DocumentBuilder
    attr_reader :sparql_client
    def initialize( sparql_client:, attachment_path_base:, logger: )
      @sparql_client = sparql_client
      @attachment_path_base = attachment_path_base
      @logger = logger
    end

    # Retrieves a document to index from the available parameters.  Is
    # capable of coping with uri identification schemes, with
    # properties as configured in the user's config, as well as with
    # optional allowed_groups.
    #
    # This is your one-stop shop to fetch all info to index a document.
    #
    #   - uri: URI of the item to fetch
    #   - properties: Array of properties as configured in the user's
    #     configuration file.
    def fetch_document_to_index( uri: nil, properties: nil )
      pipeline = false
      # we include uuid because it may be used for folding
      unless properties.has_key?("uuid")
        properties["uuid"] = ["http://mu.semte.ch/vocabularies/core/uuid"]
      end
      key_value_tuples = properties.collect do |key, val|
        query = make_property_query(uri, key, val)
        results = @sparql_client.query(query)
        if val.is_a? Hash
          # file attachment
          if val["attachment_pipeline"]
            key, value = parse_attachment(results, key)
            if value.is_a?(Array)
              pipeline = "#{val["attachment_pipeline"]}_array"
            else
              pipeline = val["attachment_pipeline"]
            end
            [key, value]
          # nested object
          elsif val["rdf_type"]
            parse_nested_object(results, key, val['properties'])
          end
        else
          values = results.collect do |result|
            case result[key]
            when RDF::Literal::Integer
              result[key].to_i
            when RDF::Literal::Double
              result[key].to_f
            when RDF::Literal::Decimal
              result[key].to_f
            when RDF::Literal::Boolean
              result[key].to_s.downcase == 'true'
            when RDF::Literal::Time
              result[key].to_s
            when RDF::Literal::Date
              result[key].to_s
            when RDF::Literal::DateTime
              result[key].to_s
            when RDF::Literal
              result[key].to_s
            else
              result[key].to_s
            end
          end
          [key, denumerate(values)]
        end
      end

      document = Hash[key_value_tuples]
      return document, pipeline
    end


    private

    def log
      @logger
    end

    # Converts the string predicate from the configuration into a portion
    # for the path used in a SPARQL query.
    #
    # The strings configered in the config file may have a ^ sign to
    # indicate inverse.  If that exists, we need to interpolate the URI.
    #
    #   - predicate: Predicate to be escaped.
    def predicate_string_term predicate
      MuSearch::SPARQL.predicate_string_term(predicate)
    end

    # Coverts the SPARQL predicate definition from the config into a
    # triple path.
    #
    # The configuration in the configuration file may contain an inverse
    # (using ^) and/or a list (using the array notation).  These need to
    # be converted into query paths so we can correctly fetch the
    # contents.
    #
    #   - predicate: Predicate definition as supplied in the config file.
    #     Either a string or an array.
    #
    # TODO: I believe the construction with the query paths leads to
    # incorrect invalidation when delta's arrive. Perhaps we should store
    # the relevant URIs in the stored document so we can invalidate it
    # correctly when new content arrives.
    def make_predicate_string predicate
      MuSearch::SPARQL.make_predicate_string(predicate)
    end

    # Constructs a SPARQL query which selects all requested properties for
    # an ElasticSearch document.
    #
    #   - uri: URI of the instance as a string
    #   - properties: properties to be discovered, as an array of
    #     definitions supplied from the configuration.
    def make_property_query( uri, property_key, property_predicate )
      predicate = property_predicate.is_a?(Hash) ? property_predicate["via"] : property_predicate
      predicate_s = make_predicate_string predicate
      <<SPARQL
    SELECT DISTINCT ?#{property_key} WHERE {
     #{sparql_escape_uri(uri)} #{predicate_s} ?#{property_key}
    }
SPARQL
    end


    # helper function for fetch_document_to_index
    # retrieves the content of a linked resource
    def parse_nested_object(results, key, properties)
      links = results.collect do |result|
        link_uri = result[key]
        if link_uri
          document, _ = fetch_document_to_index(uri: link_uri, properties: properties)
          document
        else
          nil
        end
      end

      [key, denumerate(links)]
    end




    # utility function
    def denumerate(results)
      case results.length
      when 0 then nil
      when 1 then results.first
      else results
      end
    end


    def process_file(file_path)
      begin
        File.open(file_path, "rb") do |file|
          Base64.strict_encode64 file.read
        end
      rescue Errno::ENOENT, IOError => e
        log.warn "Error reading \"#{file_path}\": #{e.inspect}"
        nil
      rescue StandardError => e
        log.warn e
        log.warn "Failed to parse attachment #{file_path}"
        nil
      end
    end

    # helper function for fetch_document_to_index
    # retrieves content of the linked attachments
    # TODO: Consider whether it's appropriate to force_encode UTF-8 for attachments
    def parse_attachment( results, key )
      attachments = results.collect do |result|
        file_path = result[key]
        if file_path
          file_path = File.join(@attachment_path_base, file_path.to_s.sub("share://",""))
          filesize = File.size(file_path)
          if filesize < ENV['MAXIMUM_FILE_SIZE'].to_i
            process_file(file_path)
          else
            log.warn "Ignoring attachment #{file_path}: #{filesize} bytes exceeds allowed size of #{ENV["MAXIMUM_FILE_SIZE"]} bytes"
            nil
          end
        else
          nil
        end
      end

      case attachments.length
      when 0
        [key, ""]
      when 1
        [key, attachments.first]
      else
        attachments = attachments.keep_if { |v| v } # filter out falsy values (If one of the array is falsy, others are not taken into account)
        [key, attachments.collect { |attachment| { data: attachment} }]
      end
    end
  end
end
