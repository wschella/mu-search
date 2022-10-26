# Returns a string representation for an authorization group
# E.g. { "name": "department", "variables": ["legal", "europe"] }
#      will be serialized to "departmentlegaleurope"
def serialize_authorization_group(group)
  group["name"] + group["variables"].join("")
end

# Returns a string representation for a list of authorization groups
# E.g. [
#        { "name": "public", "variables": [] },
#        { "name": "department", "variables": ["legal", "europe"] }
#      ]
#      will be serialized to "departmentlegaleurope#public"
def serialize_authorization_groups(groups)
  groups.map { |group| serialize_authorization_group group }.sort.join("#")
end

# Sorts a given list of authorization groups
# E.g. [
#        ["name": "public", "variables": [] ],
#        ["name": "admin", "variables": [] ],
#        ["name": "department", "variables": ["legal"] ],
#        ["name": "department", "variables": ["finance"] ]
#      ]
# will become
#      [
#        ["name": "admin", "variables": [] ],
#        ["name": "department", "variables": ["finance"] ],
#        ["name": "department", "variables": ["legal"] ],
#        ["name": "public", "variables": [] ]
#      ]
# Note: the list of variables in an authorization group
#       is already ordered and should not be sorted alphabetically
def sort_authorization_groups(groups)
  groups.sort_by { |group| serialize_authorization_group group }
end

# Get the allowed groups from an incoming HTTP request.
# Returns nil if they are not set
def get_allowed_groups
  allowed_groups_s = request.env["HTTP_MU_AUTH_ALLOWED_GROUPS"]
  if allowed_groups_s.nil? || allowed_groups_s.length == 0
    nil
  else
    allowed_groups = JSON.parse(allowed_groups_s)
    sort_authorization_groups allowed_groups
  end
end

# Get the allowed groups from an incoming HTTP request
# or calculate them by executing a query to the database
# if they're not set yet.
def get_allowed_groups_with_fallback
  allowed_groups = get_allowed_groups
  if allowed_groups.nil?
    # TODO: this isn't very clean and relies on ruby-template internals
    # - Send simple query to mu-auth
    query("ASK {?s ?p ?o}")
    # - Parse allowed groups from mu-ruby-template internals
    allowed_groups = JSON.parse(RequestStore.store[:mu_auth_allowed_groups])
    sort_authorization_groups allowed_groups
  else
    allowed_groups
  end
end
