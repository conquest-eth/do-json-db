# do-json-db

A DB for cloudflare worker using Durable Object key-value db to store and queries json objects

## Implementation Design

### first version: zero schema


#### storing
store a json and index every field

json need to have an `id` field as string, this will be used to retrieve the whole object from a single get

json can have a `typeName` which if present is prepended to the id to form the actual id stored in the DUrable Object query. this allow to query json object based on type

every other field are assumed to be indexable. (long string are not acceptable)

Optionally, we could have field startign with underscore, these do not get indexed

For every indexed field, we store a reference to the `id`

#### querying

When we query we can provide a list of field and their value

this will perform a query for each on the indexed field and merge the result so that only the json object which fit all criteria are selected


