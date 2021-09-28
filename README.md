# Rafty

A distributed key-value store based on the [Raft Consensus Algorithm](https://raft.github.io) and [RocksDB](http://rocksdb.org) for storage.

:rotating_light: This is being done for demonstration/learning purposes, use at your own risk :rotating_light:

## Overview
The project makes use of the following tools:

* [Integrant](https://github.com/weavejester/integrant) for managing the components lifecycle
* [RocksDB](http://rocksdb.org) as the storage layer
* API : [Ring](https://github.com/ring-clojure/ring) with jetty, [Reitit](https://github.com/metosin/reitit) for the routing and [Malli](https://github.com/metosin/malli) for request/response coercion

## Architecture

![Rafty Architecture](rafty.png)
## Run

The project uses [lein](https://leiningen.org), so you'll need to have that installed in order to run the project

Before running the project, make sure to edit the configuration file to suit your needs in `resources/config.edn`

You can also set the configuration through environment variables

```bash
# The API server's port
export KVSTORE_PORT=3000

# Persistent storage location
export KVSTORE_STORAGE_DIR=/tmp/rocksdb 

# Peers addresses in the format: "id1@address1;id2@address2;..."
export KVSTORE_PEERS="2@http://localhost:3002;3@http://localhost:3003"

# The current server's id
export KVSTORE_ID=1
```

You can then start a node through lein

```
lein run -m kvstore.server resources/config.edn
```

## Scope of the current implementation

- [x] Leader election
- [x] Commands Log replication
- [x] Persistence
- [ ] Membership changes
- [ ] Simulated environment for testing, covering circumstances such as network loss, network partition, etc.
 
TODO:
- move the project to deps.edn
- add more tests
- ...

## More information
- [The Raft Paper](https://raft.github.io/raft.pdf)
- [Raft visualization](http://thesecretlivesofdata.com/raft/)
## License

MIT License

Copyright © 2021 Nizar <nisehl@gmail.com>

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
"Software"), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
