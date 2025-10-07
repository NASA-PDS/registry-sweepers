### Requirements

Requires a running deployment of registry

**Python Version:** This Docker container uses Python 3.13.7 to leverage the latest Python features and security updates for production deployments, while the package itself supports both Python 3.12 and 3.13 for development flexibility.

#### Env Variables
`PROV_ENDPOINT` - the URL of the registry OpenSearch http endpoint
`PROV_CREDENTIALS` - a JSON string of format `{"$username": "$password"}`
`LOGLEVEL` - (optional - defaults to `INFO`) an integer log level or anycase string matching a python log level like `INFO`
`DEV_MODE=1` - (optional) in dev mode, host cert verification is disabled


### Development

To build and run  (assuming registry local-dev defaults for host/credentials)

    cd path/to/registry-sweepers/
    docker image build --tag nasapds/registry-sweepers --file ./docker/Dockerfile .
    docker run --env PROV_ENDPOINT='https://localhost:9200/' --env PROV_CREDENTIALS='{"admin": "admin"}' nasapds/registry-sweepers

### Release of new versions

To release a new version for I&T, an updated image must be built and published to Docker Hub at `nasapds/registry-sweepers`

    cd path/to/registry-sweepers/docker
    docker image build --tag nasapds/registry-sweepers:{version} --file ./docker/Dockerfile .
    docker image push nasapds/registry-sweepers:{version}

### Production Deployment

TBD
