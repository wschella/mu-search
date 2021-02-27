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
    def initialize(logger:, index_manager:, search_configuration:, &block)
      @logger = logger
      @index_manager = index_manager
      @mutex = Mutex.new
      define_method(:handler, block) if block_given?
      @number_of_threads =
        if search_configuration[:number_of_threads] > 0
          search_configuration[:number_of_threads]
        else
          DEFAULT_NUMBER_OF_THREADS
        end

      wait_interval =
        if search_configuration[:update_wait_interval_minutes].nil?
          DEFAULT_WAIT_INTERVAL_MINUTES
        else
          search_configuration[:update_wait_interval_minutes]
        end
      @min_wait_time =  wait_interval * 60 / 86400.0

      # FIFO queue of outstanding update actions, max. 1 per subject
      @queue = []
      # In memory cache of index types to update per subject
      @subject_map = Hash.new { |hash, key| hash[key] = Set.new() }

      restore_queue_and_setup_persistence
      setup_runners

      @logger.info("UPDATE HANDLER") { "Update handler configured with #{@number_of_threads} threads and wait time of #{wait_interval} minutes" }
    end

    ##
    # add an action to the queue
    # type should be either :update or :delete
    def add(subject, index_type, type)
      @mutex.synchronize do
        # Add subject to queue if an update for the same subject hasn't been scheduled before
        if !@subject_map.has_key? subject
          @logger.debug("UPDATE HANDLER") { "Add update for subject <#{subject}> to queue" }
          @queue << { timestamp: DateTime.now, subject: subject, type: type}
        else
          @logger.debug("UPDATE HANDLER") { "Update for subject <#{subject}> already scheduled" }
        end
        @subject_map[subject].add(index_type)
      end
    end

    ##
    # add an update to be handled
    # wrapper for add
    def add_update(subject, index_type)
      add(subject, index_type, :update)
    end

    ##
    # add a delete to be handled
    # wrapper for add
    def add_delete(subject, index_type)
     add(subject, index_type, :delete)
    end

    private

    # Setup a runner per thread to handle updates
    def setup_runners
      @runners = (0...@number_of_threads).map do |i|
        Thread.new(abort_on_exception: true) do
          @logger.debug("UPDATE HANDLER") { "Runner #{i} ready for duty" }
          while true do
            change = subject = index_types = type = nil
            begin
              @mutex.synchronize do
                if @queue.length > 0 && (DateTime.now - @queue[0][:timestamp]) > @min_wait_time
                  change = @queue.shift
                  subject = change[:subject]
                  type = change[:type]
                  index_types = @subject_map.delete(subject)
                end
              end
              if !change.nil?
                large_queue = 500
                if @queue.length > large_queue and large_queue % @number_of_threads == 0
                  # log only once per nb_of_threads
                  @logger.warn("UPDATE HANDLER") { "Large number of updates (#{@queue.length}) in queue" }
                end
                @logger.debug("UPDATE HANDLER") { "Handling update of #{subject}" }
                handler(subject, index_types, type)
              end
            rescue StandardError => e
              @logger.error("UPDATE HANDLER") { "Update of subject <#{subject}> failed" }
              @logger.error("UPDATE HANDLER") { e.full_message }
            end
            sleep 5
          end
        end
      end
    end

    # Initializes the update queue and ensures the queue is persisted on disk at regular intervals
    def restore_queue_and_setup_persistence
      @store = YAML::Store.new("/config/update-handler.store", true)
      @store.transaction do
        @queue = @store.fetch("queue", [])
        @subject_map = @subject_map.merge(@store.fetch("index", {}))
        @logger.info("UPDATE HANDLER") { "Restored update queue (length: #{@queue.length})" }
      end

      @persister = Thread.new(abort_on_exception: true) do
        while true
          sleep 300
          @mutex.synchronize do
            @logger.info("UPDATE HANDLER") { "Persisting update queue to disk (length: #{@queue.length})" }
            begin
              @store.transaction do
                @store["queue"] = @queue
                @store["index"] = @subject_map
              end
            rescue StandardError => e
              @logger.error("UPDATE HANDLER") { "Failed to persist update queue to disk" }
              @logger.error("UPDATE HANDLER") { e.full_message }
            end
          end
        end
      end
    end
  end
end

