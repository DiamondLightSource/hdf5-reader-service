# Run in a container

Pre-built containers with hdf5-reader-service and its dependencies already
installed are available on [Github Container Registry](https://ghcr.io/DiamondLightSource/hdf5-reader-service).

## Starting the container

To pull the container from github container registry and run:

```
$ docker run ghcr.io/diamondlightsource/hdf5-reader-service:latest --version
```

To get a released version, use a numbered release instead of `latest`.
