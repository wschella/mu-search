module MuSearch
  module SPARQL
    class ClientWrapper
      def initialize(logger:, sparql_client:, options:)
        @logger = logger
        @sparql_client = sparql_client
        @options = options
      end

      def query(query_string)
        @logger.debug("SPARQL") { "Executing query with #{@options.inspect}\n#{query_string}" }
        @sparql_client.query query_string, @options
      end

      def update(query_string)
        @logger.debug("SPARQL") { "Executing update with #{@options.inspect}\n#{query_string}" }
        @sparql_client.update query_string, @options
      end
    end

    class ConnectionPool
      ##
      # default number of threads to use for indexing and update handling
      DEFAULT_NUMBER_OF_THREADS = 2

      def initialize(logger:, number_of_threads:)
        @logger = logger
        number_of_threads = DEFAULT_NUMBER_OF_THREADS if number_of_threads <= 0
        @sparql_connection_pool = ::ConnectionPool.new(size: number_of_threads, timeout: 3) do
          ::SPARQL::Client.new(ENV['MU_SPARQL_ENDPOINT'])
        end
        @logger.info("SETUP") { "Setup SPARQL connection pool with #{@sparql_connection_pool.size} connections. #{@sparql_connection_pool.size} connections are available." }
      end

      def up?
        begin
          sudo_query "ASK { ?s ?p ?o }", 1
        rescue StandardError => e
          false
        end
      end

      ##
      # perform an update with access to all data
      def sudo_query(query_string, retries = 6)
        begin
          with_sudo do |sudo_client|
            sudo_client.query query_string
          end
        rescue StandardError => e
          next_retries = retries - 1
          if next_retries == 0
            raise e
          else
            @logger.warn("SPARQL") { "Could not execute sudo query (attempt #{6 - next_retries}): #{query_string}" }
            timeout = (6 - next_retries)**2
            sleep timeout
            sudo_query query_string, next_retries
          end
        end
      end

      ##
      # perform an update with access to all data
      def sudo_update(query_string, retries = 6)
        begin
          with_sudo do |sudo_client|
            sudo_client.update query_string
          end
        rescue StandardError => e
          next_retries = retries - 1
          if next_retries == 0
            raise e
          else
            @logger.warn("SPARQL") { "Could not execute sudo query (attempt #{6 - next_retries}): #{query_string}" }
            timeout = (6 - next_retries)**2
            sleep timeout
            sudo_update query_string, next_retries
          end
        end
      end

      ##
      # provides a client from the connection pool with the given access rights
      def with_authorization(allowed_groups, &block)
        sparql_options = {}

        if allowed_groups && allowed_groups.length > 0
          allowed_groups_s = allowed_groups.select { |group| group }.to_json
          sparql_options = { headers: { 'mu-auth-allowed-groups': allowed_groups_s } }
        end

        with_options sparql_options, &block
      end

      ##
      # provides a client from the connection pool with sudo access rights
      def with_sudo(&block)
        with_options({ headers: { 'mu-auth-sudo': 'true' } }, &block)
      end

      private

      def with_options(sparql_options)
        @sparql_connection_pool.with do |sparql_client|
          @logger.debug("SPARQL") { "Claimed connection from pool. There are #{@sparql_connection_pool.available} connections left" }
          client_wrapper = ClientWrapper.new(logger: @logger, sparql_client: sparql_client, options: sparql_options)
          yield client_wrapper
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
        "^#{sparql_escape_uri(predicate.slice(1, predicate.length))}"
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
