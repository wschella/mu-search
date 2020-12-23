module MuSearch
  module SPARQL
    ##
    # provides a client with sudo access
    def self.sudo_client
      SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], { headers: { 'mu-auth-sudo': 'true' } } )
    end

    def self.direct_query(query_string, retries = 6)
      self.sudo_query(query_string, retries)
    end

    ##
    # perform a query with access to all data
    def self.sudo_query(query_string, retries = 6)
      begin
        sudo_client.query query_string
      rescue StandardError => e
        next_retries = retries - 1
        if next_retries == 0
          raise e
        else
          log.warn "Could not execute raw query (attempt #{6 - next_retries}): #{query_string}"
          timeout = (6 - next_retries) ** 2
          sleep timeout
          sudo_query query_string, next_retries
        end
      end
    end

    # Converts the given predicate to an escaped predicate used in a SPARQL query.
    #
    # The string may start with a ^ sign to indicate inverse.
    # If that exists, we need to interpolate the URI.
    #
    #   - predicate: Predicate to be escaped.
    def self.predicate_string_term(predicate)
      if predicate.start_with? "^"
        "^#{sparql_escape_uri(predicate.slice(1,predicate.length))}"
      else
        sparql_escape_uri(predicate)
      end
    end

    # Converts the SPARQL predicate definition from the config into a
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
    def self.make_predicate_string(predicate)
      if predicate.is_a? String
        predicate_string_term(predicate)
      else
        predicate.map { |pred| predicate_string_term pred }.join("/")
      end
    end
  end
end
