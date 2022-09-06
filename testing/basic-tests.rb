require_relative 'testing'

sparql ['admin'], <<SPARQL
DELETE WHERE {
 <document5> ?p ?o
}
SPARQL

#####
# With Index Invalidation

automatic_updates false

run_test(0) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}

sparql ['admin'], <<SPARQL
INSERT DATA {
 <document5> a <http://example.org/Document>;
             <http://mu.semte.ch/vocabularies/authorization/inGroup> <group1>;
             <http://purl.org/dc/elements/1.1/title> "giraffes";
             <http://purl.org/dc/elements/1.1/description> "A document about silly Goats.";
             <http://mu.semte.ch/vocabularies/core/uuid> "009".
}
SPARQL

sleep 1

run_test(1) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}

sparql ['admin'], <<SPARQL
DELETE WHERE {
 <document5> <http://purl.org/dc/elements/1.1/title> ?title#{' '}
}
SPARQL

sleep 1

run_test(0) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}

#####
# With Automatic Updates

sleep 1

automatic_updates true

run_test(0) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}

sparql ['admin'], <<SPARQL
INSERT DATA {
 <document5> a <http://example.org/Document>;
             <http://mu.semte.ch/vocabularies/authorization/inGroup> <group1>;
             <http://purl.org/dc/elements/1.1/title> "giraffes";
             <http://purl.org/dc/elements/1.1/description> "A document about silly Goats.";
             <http://mu.semte.ch/vocabularies/core/uuid> "009".
}
SPARQL

sleep 1

run_test(1) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}

sparql ['admin'], <<SPARQL
DELETE WHERE {
 <document5> <http://purl.org/dc/elements/1.1/title> ?title#{' '}
}
SPARQL

sleep 1

run_test(0) {
  res = elastic '/documents/search?filter[title]=giraffes', ['group1']
  res["count"]
}
