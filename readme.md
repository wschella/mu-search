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
    "types": [
        {
            "type": "document",
            "on_path": "documents",
            "rdf_type": "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties": {
                "title": "<http://purl.org/dc/elements/1.1/title>",
                "description": "<http://purl.org/dc/elements/1.1/description>" 
            }
        },
        {
            "type": "user",
            "on_path": "users",
            "rdf_type": "<http://mu.semte.ch/vocabularies/core/User>",
            "properties": {
                "fullname": "<http://xmlns.com/foaf/0.1/name>",
                "bio": "<http://mu.semte.ch/vocabularies/core/biography>"
            }
         }
    ]
}
```

Properties can also be mapped to lists of predicates, corresponding to a property path in RDF.

```
        {
            "type": "document",
            "on_path": "documents",
            "rdf_type": "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties": {
                "title": "<http://purl.org/dc/elements/1.1/title>",
                "description": "<http://purl.org/dc/elements/1.1/description>",
                "interest": ["<http://application.com/interest>", "<http://purl.org/dc/elements/1.1/title>"]
            }
        }
```

### Multi-types

It is also possible to define multi-types, which combine several simple types, which must (in the current state of the application) be defined as well.

A multi-type is defined by a list of its constituent simple types, and a set of mappings per type for each of its properties. If a mapping, or the mappings object, is absent, the same property name is assumed.

```
        {
            "type": ["document", "user"],
            "on_path": "fiche",
            "properties": [
                {
                    "name": "name",
                    "mappings": {
                        "document": "title",
                        "user": "fullname"
                    }
                },
                {
                    "name": "blurb",
                    "mappings": {
                        "document": "description",
                        "user": "bio"
                    }
                }
            ]
        }
```

### Elasticsearch Mappings

The optional `es_mappings` object for each type, if present, is used verbatim to define the Elasticsearch index's mappings, as described in <https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html> and <https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html>.

```
        {
            "type": "document",
            "on_path": "documents",
            "rdf_type": "<http://mu.semte.ch/vocabularies/core/Document>",
            "properties": {...},
            "es_mappings": {
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



## Eager Indexing [not implemented yet]

Indexes can be configured to be built when the application loads.


## Automatic Index Invalidation [not implemented yet]

When used with the Delta Service, mu-elastic-search can automatically invalidate or update indexes when notified of relevant changes in the data.


## Automatic Index Updating [not implemented yet]


## Search and Configuration API

### GET `/:type/search/`

JSON-API compliant request format, intended to match the request format of mu-cl-resources. Currently, only simple Elasticsearch methods are supported, such as match, term, prefix, fuzzy, etc.

To search for `document`s on the field `name`:

```
http://localhost:8888/userdocs/search?filter[name][match]=fish
```

Pagination is specified with `page` `number` and `size`:

```
http://localhost:8888/userdocs/search?filter[name][match]=fish&page=2&size=20
```

### POST `/:type/search/`

Accepts raw Elasticsearch Query DSL, as defined at <https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html>.

### POST `/:type/index`

Re-index all documents of type `type` for the current user's authorization group.




## Environment parameters
