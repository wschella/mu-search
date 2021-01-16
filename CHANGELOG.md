# Changelog
## v0.8.0-beta.1
**Features**
- Extracting file content using external Tika service
- Configurable log levels per scope
- Improved error logging
- Add documentation on eager indexing groups and update queues

**Fixes**
- Taking authorization headers into account on index management endpoints
- Indexing of boolean values
- Allow dashes and underscores in search property names
- Ensure same index name independently of order of auth group keys

## v0.7.0
**Features**
- Add `:has:` and `:has-no:` filter flags to filter on any/no value
- Support multiple fields for the `:phrase:` and `:phrase_prefix:` filter flags
- Make request URL and headers length configurable
- Improve documentation structure and examples

**Fixes**
- Indexing of nested objects

## v0.6.3
**Fixes**
- refactored indexing operations
- use connection pool for sparql connections
