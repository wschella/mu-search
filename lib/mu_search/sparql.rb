module MuSearch
  module SPARQL
    ##
    # provides a client with sudo access
    def self.sudo_client
      SinatraTemplate::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'], { headers: { 'mu-auth-sudo': 'true' } } )
    end


    ##
    # perform a query with access to all data
    def self.direct_query(query_string, retries = 6)
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
          direct_query query_string, next_retries
        end
      end
    end

    def self.predicate_string_term(property_path)
      if property_path.start_with?("^")
        "^#{sparql_escape_uri(property_path.slice(1,property_path.length))}"
      else
        sparql_escape_uri(property_path)
      end
    end

    def self.make_predicate_string(property_path)
      if property_path.is_a? String
        predicate_string_term(property_path)
      else
        property_path.map { |pred| predicate_string_term pred }.join("/")
      end
    end
  end
end
