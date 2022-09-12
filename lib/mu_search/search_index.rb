module MuSearch
  class SearchIndex
    attr_reader :uri, :name, :type_name, :allowed_groups, :used_groups, :mutex
    attr_accessor :status
    def initialize(uri:, name:, type_name:, allowed_groups:, used_groups:)
      @uri = uri
      @name = name
      @type_name = type_name
      @allowed_groups = allowed_groups
      @used_groups = used_groups

      @status = :valid  # possible values: :valid, :invalid, :updating
      @mutex = Mutex.new
    end
  end
end
