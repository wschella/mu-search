require_relative 'update_handler'

module MuSearch
  ##
  # the invalidating update handler is a service that executes updates or deletes on indexes.
  # it will mark an index as invalid for any change that has happened on it
  class InvalidatingUpdateHandler < MuSearch::UpdateHandler
    ##
    # creates an invalidating update handler
    def initialize(search_configuration:, **args)
      @type_definitions = search_configuration[:type_definitions]
      super(search_configuration: search_configuration, **args)
    end

    # Mark complete index as invalid on update
    def handler(subject, type_names, update_type)
      type_names.each do |type_name|
        indexes = @index_manager.indexes[type_name]
        @logger.info("UPDATE HANDLER") { "Update on subject <#{subject}> makes indexes for '#{type_name}' invalid." }
        if indexes && indexes.length
          indexes.each do |_, index|
            @logger.debug("UPDATE HANDLER") { "Mark index #{index.name} as invalid." }
            index.mutex.synchronize { index.status = :invalid }
          end
        else
          @logger.debug("UPDATE HANDLER") { "No indexes for '#{type_name} found." }
        end
      end
    end
  end
end
