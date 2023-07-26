### Requirements

Requires a running deployment of registry

#### Env Variables
`PROV_ENDPOINT` - the URL of the registry OpenSearch http endpoint
`PROV_CREDENTIALS` - a JSON string of format `{"$username": "$password"}`
`LOGLEVEL` - (optional - defaults to `INFO`) an integer log level or anycase string matching a python log level like `INFO`
`DEV_MODE=1` - (optional) in dev mode, host cert verification is disabled


### Development

To build and run  (assuming registry local-dev defaults for host/credentials)

    cd path/to/registry-sweepers
    docker build -t registry-sweepers .
    docker run -e PROV_ENDPOINT='https://localhost:9200/' -e PROV_CREDENTIALS='{"admin": "admin"}' registry-sweepers

### Release of new versions

To release a new version for I&T, an updated image must be built and published to Docker Hub at `nasapds/registry-sweepers`

    cd path/to/registry-sweepers
    docker build -t nasapds/registry-sweepers .
    docker push nasapds/registry-sweepers

### Production Deployment

TBD
