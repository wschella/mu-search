# mu-elastic-search

A component to integrate authorization-aware search via Elasticsearch into the mu.semte.ch stack.

## Using mu-elastic-search 

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

To be completed...



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
            "rdf_type" : "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties" : {
                "title" : "<http://purl.org/dc/elements/1.1/title>",
                "description" : "<http://purl.org/dc/elements/1.1/description>" 
            }
        },
        {
            "type" : "user",
            "on_path" : "users",
            "rdf_type" : "<http://mu.semte.ch/vocabularies/core/User>",
            "properties" : {
                "fullname" : "<http://xmlns.com/foaf/0.1/name>",
                "bio" : "<http://mu.semte.ch/vocabularies/core/biography>"
            }
         }
    ],
    "eager_indexing_groups" : [
        ["workingGroup"],
        ["admin"]
    ]
}
```

### Property Paths

Properties can also be mapped to lists of predicates, corresponding to a property path in RDF.

```
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties" : {
                "title" : "<http://purl.org/dc/elements/1.1/title>",
                "description" : "<http://purl.org/dc/elements/1.1/description>",
                "interest" : [
                  "<http://application.com/interest>", 
                  "<http://purl.org/dc/elements/1.1/title>"
                ]
            }
        }
```

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

### Elasticsearch Mappings

The optional `es_mappings` object for each type, if present, is used verbatim to define the Elasticsearch index's mappings, as described in <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html> and <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>.

```
        {
            "type" : "document",
            "on_path" : "documents",
            "rdf_type" : "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties" : {...},
            "es_mappings" : {
                "title" : { "type" : "text" },
                "description" : { "type" : "text" }
            }
        }
```

This will be used when creating new Elasticsearch indexes, equivalent to:

```
PUT index4901823098
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


### Other configuration file parameters

**batch_size** -- number of documents loaded from the RDF store and indexed together in a single batch.

**automatic_index_updates** -- flag to apply automatic index updates instead of invalidating indexes on receiving deltas. See below.

**eager_indexing_groups** -- see below.

**eager_indexing_sparql_query** -- see below.





## Eager Indexing

Indexes can be configured to be built when the application loads.

Currently, this is done by specifying a list of `eager_indexing_groups` in the config.json file [above](#simple-types).

In the future, it will also be possible to specify them via a SPARQL query


## Automatic Index Invalidation

When used with the Delta Service, mu-elastic-search can automatically invalidate or update indexes when notified of relevant changes in the data.

Deltas are expected in the following format:

```
{
  "graph": "http://graph1",
  "delta": {
    "inserts": [
      {
        "s": "http://uri1",
        "p": "http://predicate1",
        "o": "http://object1"
      }
    ],
    "deletes": [

    ]
  }
}
```

When a delta is received in which:

- `p` corresponds to the property of given type, and
- `s` is indeed of that type (verified by querying the triple store)

then all indexes which

- correspond to that type 
- are authorized to see `s`

are invalidated. Each one will be rebuilt the next time it is searched.

## Automatic Index Updating

Alternate to automatic index invalidation, indexes can be dynamically updated on a per-document basis according to received deltas.

When a corresponding delta is received (see previous section), the document corresponding to the delta's `s` is updated (or deleted) in every index corresponding to `s`'s type(s). Note that if there are many different configurations of ALLOWED_GROUPS, this might be a large number of indexes.

Also note this is not currently a blocking operation: an update will not lock the index, so that a simultaneously received search request might be run on the un-updated index.

Automatic updates are activated via the environment variable `AUTOMATIC_INDEX_UPDATES`, or `automatic_index_updates` in the config.json file.


## Blocking and Queuing

Notes on index building and re-building, the blocking model, request priority, which requests might be forced to wait and when...


## Search and Configuration API

### POST `/:type/search`

Accepts raw Elasticsearch Query DSL, for testing and more complex queries. 

### GET `/:type/search`

JSON-API compliant request format, intended to match the request format of mu-cl-resources. A portion of the Elasticsearch Query DSL is supported. More complex queries should be sent via POST to the raw Elasticsearch Query DSL endpoint

To search for `document`s on all fields:

    http://localhost:8888/userdocs/search?filter[_all]=fish

To search for `document`s on the field `name`:

    http://localhost:8888/userdocs/search?filter[name]=fish

Multiple fields can also be searched:

    http://localhost:8888/userdocs/search?filter[name,description]=fish

A series of flags provide access to features such as term, range and fuzzy searches. Filters are expressed as terms separated by `:` before the field name(s), as follows:

    http://localhost:8888/userdocs/search?filter[:term:tag]=fish

The following flags are currently implemented:

- `:term:` Term Query
- `:terms:` Terms Query, terms should be separated by a `,`, such as:

    http://localhost:8888/userdocs/search?filter[:terms:tag]=fish,seafood

- Other Term level queries: `:prefix:`, `:wildcard:`, `:regexp:`, `:fuzzy:`
- `:query:` -- Query String Query
- single range flags `:gt:`,`lt:`, `:gte:`, `:lte:` -- Range Query
- paired range flags `:lt,gt:`, `:lte,gte:`, `:lt,gte:`, `:lte,gt:` -- Range Query, ranges limits should be separated by a `,` such as:

    http://localhost:8888/userdocs/search?filter[:lte,gte:importance]=3,7


#### Pagination

Pagination is specified with `page` `number` and `size`:

    http://localhost:8888/userdocs/search?filter[name]=fish&page=2&size=20

### POST `/:type/search`

Accepts raw Elasticsearch Query DSL, as defined at <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>.

### POST `/:type/index`

Re-index all documents of type `type` for the current user's authorization group.




## Environment parameters
