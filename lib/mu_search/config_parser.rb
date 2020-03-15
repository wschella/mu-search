module MuSearch
  module ConfigParser
    def self.parse(path)
      default_configuration = {
        batch_size: 100,
        common_terms_cutoff_frequency: 0.001,
        attachment_path_base: "/data",
        eager_indexing_groups: [],
        update_wait_interval_minutes: 1,
        number_of_threads: 1,
        enable_raw_dsl_endpoint: false,
        excluded_fields: ["data","attachment"]
      }

      json_config = JSON.parse(File.read(path))
      config = default_configuration.clone

      # the following settings can come from either ENV or the json file
      # ENV is capitalized, we ignore empty strings from ENV and nil values from both
      [
        {name: "batch_size", parser: :parse_integer},
        {name: "max_batches", parser: :parse_integer},
        {name: "persist_indexes", parser: :parse_boolean},
        {name: "additive_indexes", parser: :parse_boolean},
        {name: "enable_raw_dsl_endpoint", parser: :parse_boolean},
        {name: "automatic_index_updates", parser: :parse_boolean},
        {name: "attachments_path_base", parser: :parse_string},
        {name: "common_terms_cutoff_frequency", parser: :parse_float},
        {name: "update_wait_interval_minutes", parser: :parse_integer},
        {name: "number_of_threads", parser: :parse_integer},
        {name: "excluded_fields", parser: :parse_string_array}
      ].each do |setting|
        name = setting[:name]
        value = self.send(setting[:parser], ENV[name.upcase], json_config[name])
        unless value.nil?
          config[name.to_sym] =  value
        end
      end

      config[:default_index_settings] = json_config["default_settings"]
      if json_config["eager_indexing_groups"]
        config[:eager_indexing_groups]  = json_config["eager_indexing_groups"]
      end
      config[:index_config] = json_config["types"]
      config[:type_paths] = Hash[
        json_config["types"].collect do |type_def|
          [type_def["on_path"], type_def["type"]]
        end
      ]
      config[:type_definitions] = Hash[
        json_config["types"].collect do |type_def|
          [type_def["type"], type_def]
        end
      ]
      config[:master_mutex] = Mutex.new
      config
    end

    ##
    # will return the first non nil value which was correctly returned by the provided block
    # usage:
    #  as_type("a", "number", "of", "values") do |value|
    #    Float(value)
    #  end
    #
    def self.as_type(*possible_values, &block)
      while possible_values.length > 0
        value = possible_values.shift
        begin
          unless value.nil?
            return yield(value)
          end
        end
      end
    end

    def self.parse_string(*possible_values)
      as_type(*possible_values) do |val|
        val.to_s
      end
    end

    
    def self.parse_string_array(*possible_values)
      as_type(*possible_values) do |val|
        val.each { |s| s.to_s }
      end
    end

    def self.parse_float(*possible_values)
      as_type(*possible_values) do |val|
        Float(val)
      end
    end

    def self.parse_integer(*possible_values)
      as_type(*possible_values) do |val|
        Integer(val)
      end
    end

    def self.parse_boolean(*possible_values)
      as_type(*possible_values) do |val|
        if val.kind_of?(String) && ! val.strip.empty?
          ["true","True","TRUE"].include?(val)
        else
          val
        end
      end
    end
  end
end
