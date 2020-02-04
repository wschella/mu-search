require_relative 'update_handler'

module MuSearch
  ##
  # the invalidating update handler is a service that executes updates or deletes on indexes.
  # it will mark an index as invalid for any change that has happened on it
  class InvalidatingUpdateHandler < MuSearch::UpdateHandler

    ##
    # creates an invalidating update handler
    def initialize(logger: , type_definitions:)
      @type_definitions = type_definitions
      super(logger: logger)
    end

    def handler(subject, index_types, type)
      index_types.each do |index_type|
        indexes = Indexes.instance.get_indexes(index_type)
        indexes.each do |key, index|
          @logger.debug "#{subject} is part of #{index[:index]}, Invalidating #{index[:index]}"
          Indexes.instance.mutex(index[:index]).synchronize do
            Indexes.instance.set_status(index[:index], :invalid)
          end
        end
      end
    end
  end
end
