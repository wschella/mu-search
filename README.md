# mu-search

A component to integrate authorization-aware search via Elasticsearch into the mu.semte.ch stack.

## Table of Contents

- [Using mu-elastic-search](#using-mu-elastic-search)
- [Access Rights](#access-rights)
- [Configuration](#configuration)
  - [Simple Types](#simple-types)
  - [Property Paths](#property-paths)
  - [Nested Objects](#nested-objects)
  - [Multi-types](#multi-types)
  - [Elasticsearch Settings](#elasticsearch-settings)
  - [Elasticsearch Mappings](#elasticsearch-mappings)
- [Index Lifecycle](#index-lifecycle)
  - [Persistent Indexes](#persistent-indexes)
  - [Eager Indexing](#eager-indexing)
  - [Additive Indexes](#additive-indexes)
  - [Automatic Index Invalidation](#automatic-index-invalidation)
  - [Automatic Index Updating](#automatic-index-updating)
- [Indexing Attachments](#indexing-attachments)
- [Blocking and Queuing](#blocking-and-queuing)
- [Examples](#examples)
- [API](#api)
- [Environment Parameters](#environment-parameters)

## Using mu-elastic-search 

### Memory Usage

The Elasticsearch Docker image requires a lot of memory. This can be set locally:

```
sysctl -w vm.max_map_count=262144
```

### Setup

First, add mu-elastic-search and Elasticsearch to your docker-compose file.  A link must be made to the folder containing the configuration file. (The current example is with a local build, since no image has been published yet on docker-hub.)

```
  mu-elastic:
    build: ./mu-elastic-search
    ports:
      - 8888:80
    links:
      - db:database
    volumes:
      - ./config/mu-elastic-search:/config
  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:6.4.1
    volumes:
      - ./data/elasticsearch:/usr/share/elasticsearch/data
    ports:
      - 9200:9200
```

Then, create the `./config/mu-elastic-search` directory, copy `config.json` into it, and modify this file to define how RDF triples are indexed as Elasticsearch documents, as described [below](#configuration).


## Access Rights

Access rights are determined according to the contents of two headers, `MU_AUTH_ALLOWED_GROUPS` and `MU_AUTH_USED_GROUPS`.

Currently, a separate Elasticsearch index is created for each combination of document type and authorization group.  

[To be completed...]



## Configuration

The configuration file `config.json` is used to specify the mapping between RDF triples and Elasticsearch documents, as well as other parameters.

### Simple Types

Simple types are defined as an object containing the type name, path name, RDF type, and a set of mappings between properties and RDF predicates. 

Here is a simple example of a complete `config.json` file.

```
{
    "types" : [
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://mu.semte.ch/vocabularies/core/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description" 
            }
        },
        {
            "type" : "user",
            "on_path" : "users",
            "rdf_type" : "http://mu.semte.ch/vocabularies/core/User",
            "properties" : {
                "fullname" : "http://xmlns.com/foaf/0.1/name",
                "bio" : "http://mu.semte.ch/vocabularies/core/biography"
            }
         }
    ],
    "eager_indexing_groups" : [
        ["workingGroup"],
        ["admin"]
    ]
}
```

*NOTE*: there are two protected fields that should not be used as property keys: "uuid" and "uri" are both used by musearch. they are used to store the uuid and uri of the root resource.

### Property Paths

Properties can also be mapped to lists of predicates, corresponding to a property path in RDF.

```
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://mu.semte.ch/vocabularies/core/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description",
                "interest" : [
                  "http://application.com/interest", 
                  "http://purl.org/dc/elements/1.1/title"
                ]
            }
        }
```

### Nested Objects

Objects can be nested to arbitrary depth, using the `"via"` field:

```
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://mu.semte.ch/vocabularies/core/Document",
            "properties" : {
                "title" : "http://purl.org/dc/elements/1.1/title",
                "description" : "http://purl.org/dc/elements/1.1/description",
                "author" : {
                  "via" : "http://purl.org/dc/elements/1.1/creator",
                  "rdf_type" : "http://xmlns.com/foaf/0.1/Person",
                  "properties" : {
                     "name" : ...
                  }
                }
            }
        }
```

Note that nested objects cannot contain file attachments.

### Multi-types

It is also possible to define multi-types, which combine several simple types. Currently, these simple types must be defined as well. (It should also be possible to define fully inline subtypes.)

A multi-type is defined by a list of its constituent simple types, and a set of mappings per type for each of its properties. If a mapping, or the mappings object, is absent, the same property name is assumed.

```
        {
            "type" : "fiche",
            "composite_types" : ["document", "user"],
            "on_path" : "fiches",
            "properties" : [
                {
                    "name" : "name",
                    "mappings" : {
                        "document" : "title",
                        "user" : "fullname"
                    }
                },
                {
                    "name" : "blurb",
                    "mappings" : {
                        "document" : "description",
                        "user" : "bio"
                    }
                }
            ]
        }
```

### Elasticsearch Settings

Elasticsearch index settings can optionally be specified for the whole domain, and overridden on a per-type basis. To specify settings for all indexes, use `default_settings`:

```
  "types" : [...],
  "default_settings" : {
        "analysis": {
          "analyzer": {
            "dutchanalyzer": {
              "tokenizer": "standard",
              "filter": ["lowercase", "dutchstemmer"] } },
          "filter": {
            "dutchstemmer": {
              "type": "stemmer",
              "name": "dutch" } } } }
```

To specify them for a single type, use `settings`:

```
  "types": [
    {
      "type": "agendaitems"
      ...
      "settings" : {
        "analysis": {
          "analyzer": {
            "dutchanalyzer": {
              "tokenizer": "standard",
              "filter": ["lowercase", "dutchstemmer"] } },
          "filter": {
            "dutchstemmer": {
              "type": "stemmer",
              "name": "dutch" } } } }
    }
    ...
```

### Elasticsearch Mappings

The optional `es_mappings` object for each type, if present, is used verbatim to define the Elasticsearch index's mappings, as described in <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html> and <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>.

```
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "http://mu.semte.ch/vocabularies/core/Document",
            "properties" : {...},
            "es_mappings" : {
                "title" : { "type" : "text" },
                "description" : { "type" : "text" }
            }
        }
```

This will be used when creating new Elasticsearch indexes, equivalent to:

```
PUT /index4901823098
{
    "settings" : {
        "number_of_shards" : 1
    },
    "mappings" : {
        "_doc" : {
            "properties" : {
                "title" : { "type" : "text" },
                "description" : { "type" : "text" }
            }
        }
    }
}
```



## Index Lifecycle

In the base scenario, indexes are created on an as-needed basis, whenever a new search profile (authorization rights and data type) is received. Indexes can be manually re-indexed by triggering the `POST /:type/index` endpoint (see [below)(#api)).

When an index is created, it is registered in the triplestore in the `<http://mu.semte.ch/authorization>` graph. On startup, all existing indexes are deleted, since data might have changed in the meantime. 

### Persistent Indexes

In production environments, it might be costly to regenerate large indexes. If it is possible to guaranty that no data has changed while the application was down, indexes can be persisted between sessions by setting the `persist_indexes` parameter to `true`.

### Dynamic Reload (Debug)

In development mode (setting the environment parameter `RACK_ENV` to `development`), the application will listen for changes in `config.json`. Any changes will trigger a complete reload of the full application, including deleting existing indexes, and building any default indexes specified in eager indexing. This overrides persistence.

### Eager Indexing

Indexes can be configured to be built when the application loads.

Currently, this is done by specifying a list of `eager_indexing_groups` in the `config.json` file [above](#simple-types).

In the future, it will also be possible to specify them via a SPARQL query


### Additive Indexes

Indexes can be configured to be additive by setting `additive_indexes` to `true` in the configuration file or as a Docker parameter. In this mode, one index will be created per type and authorization group, and searches with headers containing multiple authorization groups will be effectuated on the combination of those indexes. This presumes that the contents  the authorization groups are mutually exclusive, or there will be duplicate results.

To be consistent, `eager_indexing_groups` are still listed as lists of lists of groups, even though each list is a singleton:

```
  "eager_indexing_groups" : [[{"name" : "documents", "variables" : ["human"]}],
                             [{"name" : "documents", "variables" : ["chicken"]}]],
```

This configuration specifies that, for a given document type, two indexes will be created at startup, one for each group. Then the following search will be effectuated on the combination of those two indexes, not on a third combined index, as is the case in the default mode:

```
curl -H "MU_AUTH_ALLOWED_GROUPS: [{\"name\" : \"documents\", \"variables\" : [\"human\"]}, {\"name\" : \"documents\", \"variables\" : [\"chicken\"]}]" "http://localhost:8888/documents/index"
```

See more: https://www.elastic.co/guide/en/elasticsearch/reference/current/multi-index.html


### Automatic Index Invalidation

When used with the Delta Service, mu-elastic-search can automatically invalidate or update indexes when notified of relevant changes in the data.

Deltas are expected in the [v0.0.1 format](https://github.com/mu-semtech/delta-notifier/#v001) of the delta notifier.

```
    [
      { "inserts": [{"subject": { "type": "uri", "value": "http://mu.semte.ch/" },
                     "predicate": { "type": "uri", "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" },
                     "object": { "type": "uri", "value": "https://schema.org/Project" }}],
        "deletes": [] }
    ]
```

When a delta is received that matches a property or rdf type of an index, those indexes are invalidated. Each one will be rebuilt the next time it is searched.

### Automatic Index Updating

Alternate to automatic index invalidation, indexes can be dynamically updated on a per-document basis according to received deltas.

When a corresponding delta is received (see previous section), the document corresponding to the delta is updated (or deleted) in every index corresponding to the delta. Note that if there are many different configurations of ALLOWED_GROUPS, this might be a large number of indexes.

Also note this is not currently a blocking operation: an update will not lock the index, so that a simultaneously received search request might be run on the un-updated index.

Automatic updates are activated via the environment variable `AUTOMATIC_INDEX_UPDATES`, or `automatic_index_updates` in the `config.json` file.


## Indexing Attachments

Basic indexing of PDF, Word etc. attachments is provided using Elasticsearch's [Ingest Attachment Processor Plugin](https://www.elastic.co/guide/en/elasticsearch/plugins/current/ingest-attachment.html). Note that this is under development and liable to change.

### Create Attachment Pipeline

Attachment pipelines need to be created in Elasticsearch. A default pipeline called "attachment" is created at build time.

### Data

Currently, only indexing of local files is supported. Files must be present in the docker volume `/data`, as in this excerpt from the docker-compose file:

```
    volumes:
      - ./data/files:/data
```

and the pathname specified in the RDF data:

```
  <DOCUMENT> mu:filename "pdf-sample.pdf"
```

This should be extended to reflect URIs and other data storage practices.

### Configuration

In the configuration, a field is defined as an attachment as follows:

```
             "data" : {
                 "via" : "http://mu.semte.ch/vocabularies/core/filename",
                 "attachment_pipeline" : "attachment"
             }
```

Note that `via` can be a predicate or list of predicates, as with regular definitions.

### Searching

Searching is done on the defined field name, as any other field:

```
/documents/search?filter\[data\]=Adobe"
```



## Blocking and Queuing

[To be completed: Notes on index building and re-building, the blocking model, request priority, which requests might be forced to wait and when...]

## Examples

Examples based on the sample `config.json`. More detailed search examples are given [below](#get-typesearch).

Manually trigger (re)indexing:

```
curl -H "MU_AUTH_ALLOWED_GROUPS: [{\"name\" : \"documents\", \"variables\" : [\"human\"]}]" "http://localhost:8888/documents/index"
```

Search `title` field:

```
curl -H "MU_AUTH_ALLOWED_GROUPS: [{\"name\" : \"documents\", \"variables\" : [\"human\"]}]" "http://localhost:8888/documents/search?filter\[title\]=schappen"
```

Search for documents with version greater than 1, using [tags](#other-search-methods):

```
curl -H "MU_AUTH_ALLOWED_GROUPS: [{\"name\" : \"documents\", \"variables\" : [\"human\"]}]" "http://localhost:8888/documents/search?filter\[:gt:document_version\]=1"
```


## API

### POST `/:type/search`

Accepts raw Elasticsearch Query DSL, for testing purposes, and sending more complex queries than can be expressed with the JSON-API endpoint. 

### GET `/:type/search`

JSON-API compliant request format, intended to match the request format of mu-cl-resources. A portion of the Elasticsearch Query DSL is supported, via the `filter`, `page`, and `sort` query parameters. More complex queries should be sent via POST to the raw Elasticsearch Query DSL endpoint.

To search for `document`s on all fields:

    /documents/search?filter[_all]=fish

To search for `document`s on the field `name`:

    /documents/search?filter[name]=fish

Multiple fields can also be searched:

    /documents/search?filter[name,description]=fish

#### Other Search Methods

A series of flags provide access to features such as term, range and fuzzy searches. Filters are expressed as terms separated by `:` before the field name(s), as follows:

    /documents/search?filter[:term:tag]=fish

The following flags are currently implemented:

- `:term:` Term Query
- `:terms:` Terms Query, terms should be separated by a `,`, such as:

    /documents/search?filter[:terms:tag]=fish,seafood

- Other Term level queries: `:prefix:`, `:wildcard:`, `:regexp:`, `:fuzzy:`
- `:phrase:` and `:phrase_prefix:` -- Match Phrase and Match Phrase Prefix queries
- `:query:` -- Query String Query
- single range flags `:gt:`,`lt:`, `:gte:`, `:lte:` -- Range Query
- paired range flags `:lt,gt:`, `:lte,gte:`, `:lt,gte:`, `:lte,gt:` -- Range Query, ranges limits should be separated by a `,` such as:

    /documents/search?filter[:lte,gte:importance]=3,7

- `:common:` -- Common Terms Query. `cutoff_frequency` and `minimum_should_match` can be specified with a `,`, in that order. The former can be set application-wide in the configuration.

    /documents/search?filter[:common:description]=a+cat+named+Barney

    /documents/search?filter[:common,0.002:description]=a+cat+named+Barney

    /documents/search?filter[:common,0.002,2:description]=a+cat+named+Barney


#### Sorting

Sorting results is done using the `sort` parameter, specifying the field and `asc` or `desc`:

    /documents/search?filter[name]=fish&sort[priority]=asc

Flags can be used to specify Elasticsearch sort modes (min, max, sum, avg, median):

    /documents/search?filter[name]=fish&sort[:avg:score]=asc

Note that sorting cannot be done on text fields, unless fielddata is enabled (not recommended). Keyword and numerical data types (declared in the [type mapping](#elasticsearch-mappings)) are recommended.

#### Pagination

Pagination is specified with `page[number]` and `page[size]`:

    /documents/search?filter[name]=fish&page[number]=2&page[size]=20

#### Removing Duplicate Results

When querying multiple indexes with additive indexes, identical documents may be returned multiple times. Unique results can be assured using Elasticsearch's Field Collapsing, toggled using the `collapse_uuids` parameter:

    /documents/search?filter[name]=fish&collapse_uuids=t

Note that in the results, `count` still designates total non-unique results.

### POST `/:type/index`

Re-index all documents of type `type`. If the request is sent with authorization headers, only the authorized indexes are re-indexed. Otherwise, all pertaining indexes are triggered.

### DELETE `/:type/delete`

Delete index(es) of type `type`. If the request is sent with authorization headers, only the authorized indexes are deleted. Otherwise, all pertaining indexes are deleted.



## Environment parameters

Environment parameters can be set in the `config.json` file (lowercase) or Docker (UPPERCASE).

**batch_size** -- number of documents loaded from the RDF store and indexed together in a single batch.

**automatic_index_updates** -- flag to apply automatic index updates instead of invalidating indexes on receiving deltas.

**persist_indexes** -- when `true`, do not delete existing indexes on startup.

**common_terms_cutoff_frequency** -- default cutoff frequency for Common Terms Query.

**eager_indexing_groups** (`config.json` only) -- list of lists of groups to be indexed at startup. 

**eager_indexing_sparql_query** (`config.json` only) -- To be defined. 

**enable_raw_dsl_endpoint** -- enables the raw Elasticsearch DSL endpoint. This endpoint is disabled by default for security concerns.

