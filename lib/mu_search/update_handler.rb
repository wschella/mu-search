require 'set'

module MuSearch
  ##
  # the update handler is a service that executes updates or deletes on indexes.
  # updates are collected in a FIFO queue and executed after a certain wait interval has expired
  # NOTE: recommend use is a specific implementations:
  #  - InvalidatingUpdateHandler
  #  - AutomaticUpdateHandler
  # You can also use this class, but the handler needs to be provided as a block, e.g.
  # UpdateHandler.new(...) do |subject, index_names, type|
  # end
  class UpdateHandler
    ##
    # default interval to wait before applying changes
    DEFAULT_WAIT_INTERVAL_MINUTES = 8

    ##
    # creates an update handler
    def initialize(logger:, wait_interval: DEFAULT_WAIT_INTERVAL_MINUTES, &block)
      @logger = logger
      @min_wait_time = wait_interval * 60 / 86400.0
      @queue = []
      @index = Hash.new { |hash, key| hash[key] = Set.new() }
      @mutex = Mutex.new
      if block_given?
        define_method(:handler, block)
      end

      @runner = Thread.new(abort_on_exception: true) do
        while true do
          change = subject = index_names = type = nil
          begin
            @mutex.synchronize do
              @logger.debug "UPDATE HANDLER: #{@queue.length} updates remain to be handled"
              if @queue.length > 1000
                @logger.warn "UPDATE HANDLER: large number #{@queue.length} of updates remain to be handled"
              end
              if @queue.length > 0 && (DateTime.now - @queue[0][:timestamp]) > @min_wait_time
                change = @queue.shift
                subject = change[:subject]
                type = change[:type]
                index_names = @index.delete(subject)
              end
            end
            if ! change.nil?
              @logger.debug "UPDATE HANDLER: handling update of #{subject}"
              handler(subject, index_names, type)
            end
          rescue StandardError => e
            @logger.warn "UPDATE HANDLER: update of #{subject} failed"
            @logger.error e
          end
          sleep 30
        end
      end
    end

    ##
    # add an action to the queue
    # type should be either :update or :delete
    def add(subject, index_name, type)
      @mutex.synchronize do
        if (!@index.has_key?(subject))
          @queue << { timestamp: DateTime.now, subject: subject, type: type}
        end
        @index[subject].add(index_name)
      end
    end

    ##
    # add an update to be handled
    # wrapper for add
    def add_update(subject, index_name)
      add(subject, index_name, :update)
    end

    ##
    # add a delete to be handled
    # wrapper for add
    def add_delete(subject, index_name)
     add(subject, index_name, :delete)
    end

    def document_exists_for(document_id, rdf_type, allowed_groups)
      query = "ASK { #{sparql_escape_uri(document_id)} a #{sparql_escape_uri(rdf_type)}}"
      res = MuSearch::SPARQL.authorized_query(query, allowed_groups)
      @logger.debug "document exists: #{res.inspect}"
      res
    end
  end
end


