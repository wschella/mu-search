# mu-search

A component to integrate [authorization-aware](https://github.com/mu-semtech/mu-authorization) full-text search into a [mu.semte.ch stack](https://github.com/mu-semtech/mu-project) using [Elasticsearch](https://www.elastic.co/).

## Tutorials
### Add mu-search to a stack
The mu-search service is based on Elasticsearch. Since the Elasticsearch docker image requires a lot of memory, increase the maximum on your system by executing the following command:

```bash
sysctl -w vm.max_map_count=262144
```

Next, add the mu-search and accompanying elasticsearch service to `docker-compose.yml`

```yml
services:
  search:
    image: semtech/mu-search
    links:
      - db:database
    volumes:
      - ./config/search:/config
  elasticsearch:
    image: semtech/mu-search-elastic-backend:1.0.0
    volumes:
      - ./data/elasticsearch/:/usr/share/elasticsearch/data
```

The indices will be persisted in `./data/elasticsearch`. The `search` service needs to be linked to an instance of the [mu-authorization](https://github.com/mu-semtech/mu-authorization) service.

Create the `./config/search` directory and create a `config.json` with the following contents:

```json
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description"
            }
        },
        {
            "type" : "user",
            "on_path" : "users",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Person",
            "properties" : {
                "fullname" : "http://xmlns.com/foaf/0.1/name"
            }
         }
    ]
}
```

Finally, add the following rules to your dispatcher configuration in `./config/dispatcher.ex` to make the search endpoint available:

```elixir
  define_accept_types [
    json: [ "application/json", "application/vnd.api+json" ]
  ]

  @json %{ accept: %{ json: true } }

  get "/search/*path", @json do
    Proxy.forward conn, path, "http://search/"
  end

```

Restart the dispatcher service to pick up the new configuration
```bash
docker-compose restart dispatcher
```

Restart the stack using `docker-compose up -d`. The `elasticsearch` and `search` services will be created.

Search queries can now be sent to the `/search` endpoint. Make sure the user has access to the data according to the authorization rules.

## How-to guides
### How to persist indexes on restart
By default search indexes are deleted on (re)start of the mu-search service. This guide describes how to make sure search indexes are persisted on restart.

First, make sure the search indexes are written to a mounted volume my specifying a bind mount to `/usr/share/elasticsearch/data` on the Elasticsearch container.

```yml
services:
  elasticsearch:
    image: semtech/mu-search-elastic-backend:1.0.0
    volumes:
      - ./data/elasticsearch/:/usr/share/elasticsearch/data
```

Recreate the `elasticsearch` container by executing the following command

```bash
docker-compose up -d
```

Next, enable the persistent indexes flag in the root of the search configuration file `./config/search/config.json` of your project.
```javascript
{
  "persist_indexes": true,
  "types": [
    // index type specifications
  ]
}
```

Restart the `search` service to pick up the new configuration.

```bash
docker-compose restart search
```

Search indexes will be persisted in `./data/elasticsearch` folder and not be deleted on restart of the search service.

### How to prepare a search index on startup
[To be completed... demonstrate usage of `eager_indexing_groups`]

### How to integrate mu-seach with delta's to update search indexes
This how-to guide explains how to integrate mu-search with the delta-notification in order to automatically update search index entries when data in the triplestore is modified.

This guide assumes the [mu-authorization](https://github.com/mu-semtech/mu-authorization#add-mu-authorization-to-a-stack) and [delta-notifier](https://github.com/mu-semtech/delta-notifier) components have been added to your stack as explained in their respective installation guides.

Open the delta-notifier rules configuration `./config/delta/rules.js` and add the following rule:

```javascript
  {
    match: {
      // listen to all changes
    },
    callback: {
      url: 'http://search/update',
      method: 'POST'
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 10000,
      ignoreFromSelf: true
    }
  }
```

Enable automatic index updates (not only invalidation) in mu-search by setting the `automatic_index_updates` flag at the root of `./config/search/config.json`.

```javascript
{
  "automatic_index_updates": true,
  "types": [
     // definition of the indexed types
  ]
}
```

Restart the search and delta-notifier service.
```bash
docker-compose restart search delta-notifier
```

Any change you make in your application will now trigger a request to the `/update` endpoint of mu-search. Depending on the indexed resources and properties, mu-search will update the appropriate search index entries.

### How to specify a file's content as property
This guide explains how to make the content of files attached to a project resource searchable in the index.

This guide assumes you have already integrated mu-search in your application and configured an index for resources of type `schema:Project`.

First, mount the folder containing the files to be indexed in `/data` by adding a mounted volume on the mu-search service.

```yml
services:
  search:
    image: semtech/mu-search
    volumes:
      - ./config/search:/config
      - ./data/files:/data
```

Next, add a property `files` in the `project` type index configuration. The property `files` will hold the contents of the files.

```javascript
{
    "types" : [
        {
            "type" : "project",
            "on_path" : "projects",
            "rdf_type" : "http://schema.org/Project",
            "properties" : {
                "name" : "http://schema.org/name",
                "files" : {
                   "via" : [
                       "http://purl.org/dc/terms/hasPart",
                       "^http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource"
                   ],
                   "attachment_pipeline" : "attachment"
                 }
            }
        }
    ]
}
```
`via` expresses the path from the indexed resource to the file(s) having a URI like `<share://path/to/your/file.pdf>`.

Recreate the mu-search service using

```bash
docker-compose up -d
```

After reindex has completed, each indexed project will now contain a property `files` holding the content of the files linked to the project via `dct:hasPart/^nie:dataSource`.

Searching is done on the defined field name, `files` in this case, as on any other field:

```
GET /documents/search?filter\[files\]=open-source"
```

### How to inspect the content of a search index
The content of a search index can be inspected by running a [Kibana](https://www.elastic.co/kibana) dashboard on top of Elasticseach.

[To be completed...]

Make sure not to expose the Kibana dashboard in a production environment!

### How to reset search indexes
[To be completed...]

## Reference
### Search index configuration
Elasticsearch is used as search engine. It indexes documents according to a specified configuration and provides a REST API to search documents. The mu-search service is a layer in front of Elasticsearch that allows to specify the mapping between RDF triples and the Elasticsearch documents/properties. It also integrates with [mu-authorization](https://github.com/mu-semtech/mu-authorization) making sure users can only search for documents they're allowed to access.

This section describes how to configure the resources and properties to be indexed and how to pass Elasticsearch specific configurations and mapping in the mu-search configuration file.

#### Indexed resource types and properties
This section describes how to mapping between RDF triples and Elasticsearch documents can be specified in the mounted `/config/config.json` configuration file.

The `config.json` file contains a JSON object with a property `types`. This property contains an array of objects, one per document type that must be searchable.

```javascript
{
  "types": [
    // object per searchable document type
  ]
}
```

Note that these types do not map one-on-one with the search indexes in Elasticsearch. For each document type in the list a search index will be created **per** authorization group.

Each type object in the `types` array consists of the following properties:
- **type** : name of the type
- **on_path** : path on which the search endpoint will be published
- **rdf_type** : URI of the rdf:Class of the documents to index
- **properties** : mapping of RDF predicates to document properties
- **settings** : [type specific Elasticsearch settings](#elasticsearch-settings)
- **mappings** : [type specific Elasticsearch mapping](#elasticsearch-mappings)

`properties` contains a JSON object with a key per property in the resulting Elasticsearch document. These are the properties that will be searchable via the search API for the given resource type. The value of each key defines the mapping to RDF predicates starting from the root resource.

**WARNING**: there are two protected fields that should not be used as property keys: `uuid` and `uri`. Both are used internally by the mu-search service to store the uuid and URI of the root resource.

##### Simple properties
In the simplest scenario, the properties that need to be searchable map one-by-one on a predicate of the resource.

In the example below, a search index per user group will be created for documents and users. The documetns index contains resources of type `foaf:Document`s with a `title` and `description`. The users index contains `foaf:Person`s with only `fullname` as searchable property.

```javascript
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description"
            }
        },
        {
            "type" : "user",
            "on_path" : "users",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Person",
            "properties" : {
                "fullname" : "http://xmlns.com/foaf/0.1/name"
            }
         }
    ]
}
```

If multiple values are found in the triplestore for a given predicate, the resulting value for the property in the search document will be an array of all values.

##### Inverse properties
A property of the search document may also map to an inverse predicate. I.e. resource to be indexed is the object instead of the subject of the triple. An inverse predicate can be indicated in the mapping by prefixing the predicate URI with `^` as done in a SPARQL query.

In the example below the users index contains a property `group` that maps to the inverse predicate `foaf:member` relating a group to a user.

```javascript
{
    "types" : [
        {
            "type" : "user",
            "on_path" : "users",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Person",
            "properties" : {
                "fullname" : "http://xmlns.com/foaf/0.1/name",
                "group": "^http://xmlns.com/foaf/0.1/member"
            }
         }
    ]
}
```


##### Property paths
Properties can also be mapped to lists of predicates, corresponding to a property path in RDF. In this case, the property value is an array of strings. One string per path segment. The array starts from the indexed resource and may also include [inverse predicate URIs](#inverse-properties).

In the example below the documents index contains a property `topics` that maps to the label of the document's primary topic and a property `publishers` that maps to the names of the publishers via the inverse `foaf:publications` predicate.

```javascript
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description",
                "topics" : [
                  "http://xmlns.com/foaf/0.1/primaryTopic",
                  "http://www.w3.org/2004/02/skos/core#prefLabel"
                ],
                "publishers": [
                  "^http://xmlns.com/foaf/0.1/publications",
                  "http://xmlns.com/foaf/0.1/name"
                ]
            }
        }
    ]
}
```

##### Nested objects
A search document can contain nested objects up to an arbitrary depth. For example for a person you can nest the address object as a property of the person search document.

A nested object is defined by the following properties:
- **via** : mapping of the RDF predicate that relates the resource with the nested object. May also be an inverse URI.
- **rdf_type** : URI of the rdf:Class of the nested object
- **properties** : mapping of RDF predicates to properties for the nested object

Objects can be nested to arbitrary depth. The properties object is defined the same way as the properties of the root document, but the properties of a nested object **cannot** contain file attachments.

[Elasticsearch mappings](#elasticsearch-mappings) for nested objects must be specified in the `mappings` object at the root type using a path expression as key.

In the example below the document's creator is nested in the `author` property of the search document. The nested person object contains properties `fullname` and the current project's title as `project`.

```javascript
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description",
                "author" : {
                    "via" : "http://purl.org/dc/elements/1.1/creator",
                    "rdf_type" : "http://xmlns.com/foaf/0.1/Person",
                    "properties" : {
                        "fullname" : "http://xmlns.com/foaf/0.1/name",
                        "project": [
                            "http://xmlns.com/foaf/0.1/currentProject",
                            "http://purl.org/dc/elements/1.1/title"
                        ]
                    }
                }
            },
            "mappings": {
                "title" : { "type" : "text" },
                "author.fullname": { "type" : "text" }
            }
        }
    ]
}
```

##### File content property
To make the content of a file searchable, it needs to be indexed as a property in a search index. Basic indexing of PDF, Word etc. files is provided using [Elasticsearch's Ingest Attachment Processor Plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/ingest-attachment.html). Note that this is under development and liable to change.

The plugin is already installed in the [mu-semtech/search-elastic-backend](https://github.com/mu-semtech/mu-search-elastic-backend) image. A default ingest pipeline named `attachment` is created on startup.

Defining a property to index the content of a file requires the following keys:
- **via** : mapping of the RDF predicate (path) that relates the resource with the file(s) to index. The file URI the predicate path leads to must have a URI starting with `share://` indicating the location of the file. E.g. `<share://path/to/your/file.pdf>`.
- **attachment_pipeline** : attachment pipeline to use for indexing the files. Set to `attachment` to use the default ingest pipeline.

The example below adds a property `files` in the `project` type index configuration. The property `files` will hold the contents of the files related to the project via `dct:hasPart/^nie:dataSource`.

```javascript
{
    "types" : [
        {
            "type" : "project",
            "on_path" : "projects",
            "rdf_type" : "http://schema.org/Project",
            "properties" : {
                "name" : "http://schema.org/name",
                "files" : {
                   "via" : [
                       "http://purl.org/dc/terms/hasPart",
                       "^http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource"
                   ],
                   "attachment_pipeline" : "attachment"
                 }
            }
        }
    ]
}
```

See also "How to specify a file's content as property".

##### Multiple types
A search index can contain documents of different types. E.g. documents (`foaf:Document`) as well as creative works (`schema:CreativeWork`). Currently, each simple types the composite index is constituted of must be defined seperately in the index configuration as well.

A definition of a composite type index consists of the following properties:
- **type** : name of the composite type
- **composite_types** : list of simple type names that constitute the index
- **on_path** : path on which the search endpoint will be published
- **properties** : mapping of RDF predicates to document properties for each simple type

In contrast to the `properties` of a simple index, the `properties` of a composite index is an array. Each entry in the array is an object with the folliwng properties:
- **name** : name of property of the search document
- **mappings** : mapping to the simple type property per simple type. If the mapping for a simple type is absent, the same property name as the composite document is assumed.

The example below contains 2 simple indexes for documents and creative works, and a composite index `dossier` containing both simple index types. The composite index contains (1) a property `name` mapping to the document's `title` and creative work's `name` property respectively, and (2) a property `description` mapping to the `description` property for both simple types.

```javascript
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://xmlns.com/foaf/0.1/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description"
            }
        },
        {
            "type" : "creative-work",
            "on_path" : "creative-works",
            "rdf_type" : "http://schema.org/CreativeWork",
            "properties" : {
                "name": "http://schema.org/name",
                "description": "http://schema.org/description"
            }
         },
         {
            "type" : "dossier",
            "composite_types" : ["document", "creative-work"],
            "on_path" : "dossiers",
            "properties" : [
                {
                    "name" : "name",
                    "mappings" : {
                        "document" : "title",
                        "creative-work" : "name"
                    }
                },
                {
                    "name" : "description",
                    "mappings" : {
                        "document" : "description"
                        // mapping for 'creative-work' is missing, hence same property name 'description' is assumed
                    }
                }
            ]
         }
    ]
}
```

#### Elasticsearch settings
Elasticsearch provides a lot of [index configuration settings](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html) for analysis, logging, etc. Mu-search allows to provide this configuration for the whole domain and/or to be overridden on a per-type basis.

To specify Elasticsearch settings for all indexes, use `default_settings` next to the `types` specification:

```javascript
  "types" : [
     // definition of the indexed types
  ],
  "default_settings" : {
    "analysis": {
      "analyzer": {
        "dutchanalyzer": {
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding", "dutchstemmer"]
        }
      },
      "filter": {
        "dutchstemmer": {
          "type": "stemmer",
          "name": "dutch"
        }
      }
    }
  }
```

The content of the `default_settings` object is not elaborated here but can be found in the offical [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html). All settings provided in `settings` in the Elasticsearch configuration can be used verbatim in the `default_settings` of mu-search.

To specify Elasticsearch settings for a single type, use `settings` on the type index specification:

```javascript
{
  "types": [
    {
      "type": "document",
      "on_path": "documents",
      ...
      "settings" : {
        "analysis": {
          "analyzer": {
            "dutchanalyzer": {
              "tokenizer": "standard",
              "filter": ["lowercase", "asciifolding", "dutchstemmer"]
            }
          },
          "filter": {
            "dutchstemmer": {
              "type": "stemmer",
              "name": "dutch"
            }
          }
      }
    },
    // other type definitions
  ]
}
```

#### Elasticsearch mappings
Elasticsearch provides the option to configure [a mapping per index](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html) to specify how the properties of a document are stored and indexed. E.g. the type of the property value (string, date, boolean, ...), text-analysis to be applied on the value, etc.

In the mu-search configuration the Elasticsearch mappings can be passed via the `mappings` property per index type specification.
```javascript
{
  "types": [
    {
      "type": "document",
      "on_path": "documents",
      ...
      "mappings" : {
        "title" : { "type" : "text" },
        "description" : { "type" : "text" }
      }
    },
    // other type definitions
  ]
}
```

The content of the `mappings` object is not elaborated here but can be found in the offical [Elasticsearch documentation](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html). All settings provided in `mappings.properties` in the Elasticsearch configuration can be used verbatim in the `es_settings` of mu-search.

### Index options
In the base scenario, indexes are created on an as-needed basis, whenever a new search profile (authorization rights and data type) is received. The first search query for a new search profile may therefore take more time to complete, because the index still needs to be built. Indexes can be manually re-indexed by triggering the `POST /:type/index` endpoint (see [below](#api)).

All created indexes are stored in the triplestore in the `<http://mu.semte.ch/authorization>` graph.

#### Index metadata in the triple store
[To be completed... describe used model in the triplestore]

#### Persistent indexes
By default, on startup or restart of mu-search, all existing indexes are deleted, since data might have changed in the meantime. However, for sure in production environments, regenerating indexes might be a costly operation.

Persistence of indexes can be enabled via the `persist_indexes` flag at the root of the mu-search configuration file:
```javascript
{
  "persist_indexes": true,
  "types": [
    // index type specifications
  ]
}
```

Possible values are `true` and `false`. Defaults to `false`.

Note that if set to `true`, the indexes may be out-of-date if data has changed in the application while mu-search was down.

#### Eager indexes
Configure indexes to be pre-built when the application starts. For each user search profile for which the indexes needs to be prepared, the authorization group names and their corresponding variables needs to be passed.

```javascript
{
  "eager_indexing_groups": [
    [{"variables":[], "name":"clean"}, {"variables":["company-x"], "name":"organization-read"}, {"variables":["company-x"], "name":"organization-write"}, {"variables":[], "name":"public"}],
    [{"variables":[], "name":"clean"}, {"variables":["company-y"], "name":"organization-read"}, {"variables":[], "name":"public"}],
  ],
  "types": [
    // index type specifications
  ]
}
```

Note that if you want to prepare indexes for all user profiles in your application, you will have to provide an entry in the `eager_indexing_groups` list for **each** mutation of authorization groups and variables. For example, if you have an authorization group defining a user can only access the data of his company (hence, the company name is a variable of the authorization group), you will need to define an eager index group for each of the possible companies in your application. Another option, if the data allows to, is using [additive indexes](#additive-indexes).

#### Additive indexes
Additive indexes are indexes per authorization group where indexes are combined to respond to a search query based on the user's authorization groups. If a user is grantend access to mulitple groups, indexes will be combined to calculate the response. The additive indexes mode can only be enabled if the content of the indexes per authorization grouph are mutually exclusive. Otherwise, the search response will contain duplicate results.

Assume your application contains a company-specific user group in the authorization configuration; 2 companies: company X and company Y; and mu-search contains one search index definition for documents. If additive indexes are enabled, a search index will be generated for documents of company X and another index will be generated for documens of company Y. If a user is granted access for documents of company X as well as for documents of comany Y, a search query performed by this user will be effectuated by combining both search indexes.

Additive indexes can be enabled via the `additive_indexes` flag at the root of the mu-search configuration file. Possible values are `true` or `false`. It defaults to `false`.
```javascript
{
  "additive_indexes": true,
  "types": [
    // index type specifications
  ]
}
```

To prebuilt indexes on startup the `eager_indexing_groups` option can still be used, but each eager group entry must be singleton list:

```javascript
  "additive_indexes": true,
  "eager_indexing_groups" : [
    [ {"name" : "organization-read", "variables" : ["company-x"]} ],
    [ {"name" : "organization-read", "variables" : ["company-y"]} ]
  ],
```

### Delta integration
Mu-search integrates with the delta's generated by [mu-authorization](https://github.com/mu-semtech/mu-authorization) and dispatched by the [delta-notifier](https://github.com/mu-semtech/delta-notifier).

Follow the "How to integrate mu-seach with delta's to update search indexes" guide to setup delta notification handling for mu-search. Deltas are expected in the [v0.0.1 format](https://github.com/mu-semtech/delta-notifier/#v001) of the delta notifier.

#### Full index invalidation
By default, when a delta notification is received by mu-search, all indexes containing data related to the changes are invalidated. The index will be rebuilt the next time it is searched.

Note that a change on one resource may trigger the invalidation of multiple indexes depending on the authorization groups.

#### Partial index updates
Alternate to full index invalidation, indexes can be dynamically updated on a per-document basis according to received deltas. When a delta is received, the document corresponding to the delta is updated (or deleted) in every index corresponding to the delta. This update is not a blocking operation: an update will not lock the index, so that a simultaneously received search request might be run on the un-updated index.

Note that a change on one resource may trigger the update of multiple indexes depending on the authorization groups.

Partial index updates are enabled by setting the `automatic_index_updates` flag at the root of the search configuation:

```javascript
{
  "automatic_index_updates": true,
  "types": [
     // definition of the indexed types
  ]
}
```

### API
This section describes the REST API provided by mu-search.

In order to take access rights into account, each request requires the `MU_AUTH_ALLOWED_GROUPS` and `MU_AUTH_USED_GROUPS` headers to be present.

#### GET `/:type/search`
Endpoint to search the given `:type` index. The request format is JSON-API compliant and intended to match the request format of [mu-cl-resources](https://github.com/mu-semtech/mu-cl-resources). Search filters are passed using query params.

A subset of the [Elasticsearch Query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) is supported, via the `filter`, `page`, and `sort` query parameters. More complex queries should be sent via `POST /:type/search` endpoint.

##### Examples
To search for `document`s on all fields:

```
GET /documents/search?filter[_all]=fish
```

To search for `document`s on the field `name`:

```
GET /documents/search?filter[name]=fish
```

To search for `document`s on multiple fields, combined with 'OR':

```
GET /documents/search?filter[name,description]=fish
```

##### Supported search methods
More advanced search options, such as term, range and fuzzy searches, are supported via flags. Flags are expressed in the filter key between `:` before the field name(s). E.g. the `term` search flag looks as follows:

```
GET /documents/search?filter[:term:tag]=fish
```

The following sections list the flags that are currently implemented:

###### Term-level queries
- `:term:` : [Term query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-term-query.html)
- `:terms:` : [Terms query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-terms-query.html), terms should be comma-separated, such as: `filter[:terms:tag]=fish,seafood`
- `:prefix:` : [Prefix query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-prefix-query.html)
- `:wildcard:` : [Wildcard query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-wildcard-query.html)
- `:regexp:` : [Regexp query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-regexp-query.html)
- `:fuzzy:` : [Fuzzy query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-fuzzy-query.html)
- `:gt:`,`lt:`, `:gte:`, `:lte:` : [Range query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html)
- `:lt,gt:`, `:lte,gte:`, `:lt,gte:`, `:lte,gt:` : Combined [range query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-range-query.html), range limits should be comma-separated such as: `GET /documents/search?filter[:lte,gte:importance]=3,7`
- `:has:`: Filter on documents having any value for the supplied field. To enable the filter, it's value must be `t`. E.g. `filter[:has:translation]=t`.
- `:has-no:`: Filter on documents not having a value for the supplied field. To enable the filter, it's value must be `t`. E.g. `filter[:has-no:translation]=t`.

###### Full text queries
- `:phrase:` : [Match phrase query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html)
- `:phrase_prefix:` : [Match phrase prefix query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase-prefix.html)
- `:query:` : [Query string query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html)

- `:common:` [Common terms query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-common-terms-query.html). The flag takes additional options `cutoff_frequency` and `minimum_should_match` appended with commas such as `:common,{cutoff_frequence},{minimum_should_match}:{field}`. The `cutoff_frequency` can also be set application-wide in [the configuration file](#configuration-options).

Currently searching on multiple fields is only supported for the following flag:
- `:phrase:`
- `:phrase_prefix:`

Examples

```
GET /documents/search?filter[:common:description]=a+cat+named+Barney

GET /documents/search?filter[:common,0.002:description]=a+cat+named+Barney

GET /documents/search?filter[:common,0.002,2:description]=a+cat+named+Barney
```

##### Sorting
Sorting is specified using the `sort` query parameter, providing the field to sort on and the sort direction (`asc` or `desc`):

```
GET /documents/search?filter[name]=fish&sort[priority]=asc
```

Flags can be used to specify [Elasticsearch sort modes](https://www.elastic.co/guide/en/elasticsearch/reference/current/sort-search-results.html#_sort_mode_option) to sort on multi-valued fields. The following sort mode flags are supported: `:min:`, `:max:`, `:sum:`, `:avg:`, `:median:`.

```
GET /documents/search?filter[name]=fish&sort[:avg:score]=asc
```

Note that sorting cannot be done on text fields, unless fielddata is enabled (not recommended). Keyword and numerical data types (declared in the [type mapping](#elasticsearch-mappings)) are recommended for sorting.

##### Pagination

Pagination is specified using the `page[number]` and `page[size]` query parameters:

```
GET /documents/search?filter[name]=fish&page[number]=2&page[size]=20
```

The page number is zero-based.

##### Removing duplicate results
When querying multiple indexes (with [additive indexes](#additive-indexes)), identical documents may be returned multiple times. Unique results can be assured using [Elasticsearch's search result collapsing](https://www.elastic.co/guide/en/elasticsearch/reference/7.9/collapse-search-results.html) on the `uuid` field. The search result collapsing can be toggled using the `collapse_uuids` query parameter:

```
GET /documents/search?filter[name]=fish&collapse_uuids=t
```

However, note that `count` property in the response still designates total non-unique results.

#### POST `/:type/search`
Accepts a raw [Elasticsearch Query DSL](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html) as request body to search the given `:type` index.

This endpoint is mainly intended for testing purposes and sending more complex queries than can be expressed with the `GET /:type/search` endpoint.

For security reasons, the endpoint is disabled by default. It can be enabled by setting the `enable_raw_dsl_endpoint` flag in the root of the configuration file:
```javascript
{
  "enable_raw_dsl_endpoint": true,
  "types": [
     // definition of the indexed types
  ]
}
```

#### POST `/:type/index`
Reindex index(es) of type `:type`. If the request is sent with authorization headers, only the authorized indexes are re-indexed. Otherwise, all pertaining indexes are reindexed

#### DELETE `/:type/delete`
Delete index(es) of type `:type`. If the request is sent with authorization headers, only the authorized indexes are deleted. Otherwise, all pertaining indexes are deleted.

### Configuration options
This section gives an overview of all configurable options in the search configuration file `config.json`. Most options are explained in more depth in other sections.

- (*) **persist_indexes** : flag to enable the persistence of search indexes on startup. Defaults to `false`. See [persist indexes](#persist-indexes).
- (*) **automatic_index_updates** : flag to apply automatic index updates instead of invalidating indexes on receiving deltas. Defaults to `false`. See [delta integration](#delta-integration).
- **eager_indexing_groups** : list of user search profiles (list of authorization groups) to be indexed at startup. Defaults to `[]`. See [eager indexes](#eager-indexes).
- (*) **additive_indexes** : flag to enable [additive indexes](#additive-indexes). Defaults to `false`.
- (*) **batch_size** : number of documents loaded from the RDF store and indexed together in a single batch. Defaults to 100.
- (*) **max_batches** : maximum number of batches to index. May result in an incomplete index and should therefore only be used during development. Defaults to 1.
- (*) **number_of_threads** : number of threads to use during indexing. Defaults to 1.
- (*) **update_wait_interval_minutes** : number of minutes to wait before applying an update. Allows to prevent duplicate updates of the same documents. Defaults to 1.
- (*) **common_terms_cutoff_frequency** : default cutoff frequency for a [Common terms query](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-common-terms-query.html). Defaults to 0.0001. See [supported search methods](#supported-search-methods).
- (*) **enable_raw_dsl_endpoint** : flag to enable the [raw Elasticsearch DSL endpoint](#api). This endpoint is disabled by default for security reasons.
- (*) **attachments_path_base** : path inside the Docker container where files for the attachment pipeline are mounted. Defaults to `/data`.

All options prefixed with (*) can also be configured using an UPPERCASED variant as Docker environment variables on the mu-search container. E.g. the `batch_size` option can be set via the environment variable `BATCH_SIZE`. Environment variables take precedence over settings configured in `config.json`.

In development mode (setting the environment variable `RACK_ENV` to `development`), the application will listen for changes in `config.json`. Any change will trigger a complete reload of the full application, including deleting existing indexes, and building any default indexes specified in eager indexing. This behaviour overrules the `persist_indexes` flag.

### Environment variables
This section gives an overview of all options that are configurable via environment variables. The options that can be configured in the `config.json` file as well are not repeated here. This list contains options that can only be configured via environment variables.

- **MAX_REQUEST_URI_LENGTH** : maximum length of an incoming request URL. Defaults to 10240.
- **MAX_REQUEST_HEADER_LENGTH** : maximum length of the headers of an incoming request. Defaults to 1024000.
## Discussions
### Why a custom Elasticsearch docker image?
The [mu-semtech/search-elastic-backend](https://github.com/mu-semtech/mu-search-elastic-backend) is a custom Docker image based on the official Elasticsearch image. Providing a custom image allows better control on the version of Elasticsearch, currently v7.2.0, used in combination with the mu-search service.

The custom image also makes sure the required Elasticsearch plugins, such as the ingest-attachments plugin, are already pre-installed making the integration of mu-search in your stack a lot easier.

### Authorization groups vs indexes
Access rights are determined according to the contents of two headers, `MU_AUTH_ALLOWED_GROUPS` and `MU_AUTH_USED_GROUPS`.

Currently, a separate Elasticsearch index is created for each combination of document type and authorization group.

[To be completed...]

### Blocking and queuing
[To be completed: Notes on index building and re-building, the blocking model, request priority, which requests might be forced to wait and when...]


