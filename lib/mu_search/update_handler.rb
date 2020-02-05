require 'set'
require 'yaml/store'

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
    # default number of threads to use for handling updates
    DEFAULT_NUMBER_OF_THREADS = 2

    ##
    # creates an update handler
    def initialize(logger: Logger.new(STDOUT),
                   wait_interval: DEFAULT_WAIT_INTERVAL_MINUTES,
                   number_of_threads: DEFAULT_NUMBER_OF_THREADS,
                   &block
                  )
      @logger = logger
      @min_wait_time = wait_interval * 60 / 86400.0
      @number_of_threads = number_of_threads > 0 ? number_of_threads : DEFAULT_NUMBER_OF_THREADS
      @queue = []
      @index = Hash.new { |hash, key| hash[key] = Set.new() }
      @mutex = Mutex.new
      if block_given?
        define_method(:handler, block)
      end
      restore_queue_and_setup_persistence
      setup_runners
      @logger.info "UPDATE HANDLER: configured with #{@number_of_threads} threads and wait time of #{wait_interval} minutes"
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

    private
    def setup_runners
      @runners = (0...@number_of_threads).map do |i|
        Thread.new(abort_on_exception: true) do
          @logger.debug "UPDATE HANDLER: runner #{i} ready for duty"
          while true do
            change = subject = index_names = type = nil
            begin
              @mutex.synchronize do
                if @queue.length > 500
                  @logger.info "UPDATE HANDLER: large number of updates (#{@queue.length}) to be handled"
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
            sleep 5
          end
        end
      end
    end

    def restore_queue_and_setup_persistence
      @store = YAML::Store.new("/config/update-handler.store", true)
      @store.transaction do
        @queue = @store.fetch("queue", [])
        @index = @index.merge(@store.fetch("index", {}))
        @logger.info "UPDATE HANDLER: restored queue (length: #{@queue.length})"
      end

      @persister =  Thread.new(abort_on_exception: true) do
        sleep 300
        @mutex.synchronize do
          if @queue.length > 0
            @logger.info "UPDATE HANDLER: persisting queue (length: #{@queue.length}) to disk"
            begin
              @store.transaction do
                store["queue"] = @queue
                store["index"] = @index
              end
            rescue StandardError => e
              @logger.warn "UPDATE HANDLER: failed to persist queue. #{e.message}"
              @logger.error e
            end
          end
        end
      end
    end
  end
end

