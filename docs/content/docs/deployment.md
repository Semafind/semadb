---
title: 💿 Deployment
weight: 60
---

# Deployment

SemaDB can be deployed in a variety of ways, depending on your requirements and infrastructure. This guide will walk you through the most common deployment methods.

## Standalone

You can run SemaDB as a standalone process, which is useful for development and small deployments. You can download the pre-built binaries from the [releases](https://github.com/Semafind/semadb/releases) or compile it yourself.

```bash
git clone --depth=1 https://github.com/Semafind/semadb.git
cd semadb
# Assuming you have Go installed to compile the code, or you can download the
# pre-built binaries.
go build
# Specify your own configuration file
SEMADB_CONFIG=config.yaml ./semadb
```

You can then let a service manager like [systemd](https://systemd.io/) to look after the process such as restarting it if it crashes or the server reboots.

## Docker

The most common method to deploy is to use [Docker](https://www.docker.com/) and deploy SemaDB as a single container or multiple containers in a cluster. The [package repository](https://github.com/Semafind/semadb/pkgs/container/semadb) contains the pre-built Docker images.

```bash
docker run -it --rm -v ./config:/config -e SEMADB_CONFIG=/config/semadb-config.yaml -v ./data:/data -p 8081:8081 ghcr.io/semafind/semadb:main
```

> Please use the appropriate release version tag instead of `main` so your deployment doesn't track the main branch which may contain breaking changes or development code.

When deploying, there are a few things to keep in mind:

- **Configuration:** You need to mount the configuration file and point to it using the `SEMADB_CONFIG` environment variable. There is no set place for the configuration file, so you can mount it wherever you like.
- **Data store:** SemaDB stores data in a single directory, so you need to mount a volume to store the data. Where SemaDB actually stores depends on the `rootDir` configuration. By default, the data directory is `./data` and the `semadb` executable is located at / giving `/data` as the mount point in the container.
- **Ports:** The container exposes an HTTP server on the specified port, in this case, 8081. This is the port you will use to interact with SemaDB. You can expose the RPC API and metrics port as well if you need them which are also defined in the configuration file.

## Docker Compose

Even if you are deploying as a single container, it is useful to use [Docker Compose](https://docs.docker.com/compose/) to manage the container. Here is an example `docker-compose.yml` file:

```yaml
services:
  semadb:
    image: ghcr.io/semafind/semadb:main
    # **The hostname must match the server name in the configuration file**.
    # This is how SemaDB knows which server it is itself, otherwise it will try
    # to indefinitely route requests.
    hostname: semadb
    restart: unless-stopped
    ports:
      - "8080:8080" # HTTP API as set in the configuration file
      # - "9898:9898" # RPC API
      # - "8090:8090" # Metrics
    volumes:
      - ./:/config
      - semadb_data_1:/semadb-data
    environment:
      - SEMADB_CONFIG=/config/config.yaml
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 12G
        reservations:
          cpus: '1.0'
          memory: 2G
  # Repeat the service definition for each server in the cluster although this
  # is unlikely to be on the same server. You'll probably run the above on
  # multiple servers.
  # semadb2:
  #   ...

volumes:
  semadb_data_1:
```

If you are deploying multiple instances, it is important to check whether each container can reach the others based on the hostnames in the configuration file. *You cannot have a load balancer in front of SemaDB RCP API* as it needs to know its own hostname to route requests correctly. But it is possible to have a load balancer in front of the HTTP API as each instance will route requests to the correct server.

## Kubernetes

The simplest way to run SemaDB on [Kubernetes](https://kubernetes.io/) is to deploy it as a [StatefulSet](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/). Here is an example `semadb-statefulset.yaml` file:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: semadb
  namespace: semadb
spec:
  selector:
    matchLabels:
      app: semadb
  serviceName: semadb
  replicas: 3
  podManagementPolicy: Parallel
  revisionHistoryLimit: 5
  template:
    metadata:
      labels:
        app: semadb
    spec:
      terminationGracePeriodSeconds: 40
      containers:
      - name: semadb
        image: ghcr.io/semafind/semadb:main
        resources:
          requests:
            cpu: "0.5"
            memory: 1Gi
          limits:
            cpu: "3.0"
            memory: 4Gi
        ports:
        - containerPort: 8080
          name: httpapi
        - containerPort: 9898
          name: rpcapi
        - containerPort: 8090
          name: metrics
        env:
        - name: SEMADB_CONFIG
          value: /etc/semadb/config.yaml
        volumeMounts:
          - mountPath: /semadb-data
            name: semadb-data-pvc
          - mountPath: /etc/semadb
            name: semadb-config-volume
        readinessProbe:
            failureThreshold: 3
            initialDelaySeconds: 3
            periodSeconds: 10
            successThreshold: 1
            timeoutSeconds: 10
            tcpSocket:
              host: ''
              port: 8080
      volumes:
        - name: semadb-config-volume
          configMap:
            name: semadb-config-map
            items:
              - key: semadb-config.yaml
                path: config.yaml
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
          - weight: 100
            podAffinityTerm:
              topologyKey: kubernetes.io/hostname
              labelSelector:
                matchLabels:
                  app: semadb
  volumeClaimTemplates:
  - metadata:
      name: semadb-data-pvc
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
      # storageClassName: ...
```

Along with a [Service](https://kubernetes.io/docs/concepts/services-networking/service/) to expose the StatefulSet:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: semadb
  labels:
    app: semadb
spec:
  ports:
  - port: 8080
    name: httpapi
  - port: 8090
    name: metrics
  clusterIP: None
  selector:
    app: semadb
```

Let's go over the important parts of the configuration:

- We're asking for 3 replicas of the SemaDB container. Under the service with clusterIP None, this means we'll get 3 pods with the hostname `semadb-0`, `semadb-1`, and `semadb-2` that are reachable from within the cluster. This means the servers in the configuration file should be `semadb-0.semadb:9898`, `semadb-1.semadb:9898`, and `semadb-2.semadb:9898` respectively.
- Similar to the docker compose example, we're mounting the configuration as a [config map](https://kubernetes.io/docs/concepts/configuration/configmap/) and the data as a [persistent volume claim](https://kubernetes.io/docs/concepts/storage/persistent-volumes/). In this case, the configuration is shared and the data is unique to each pod via the `volumeClaimTemplates` option.
- We're using a `podAntiAffinity` rule to ensure that the pods are spread across different nodes in the cluster. This is important to ensure all our eggs aren't in one basket.

We leave how to create the config map and persistent volume claims as they may be infrastructure and tooling dependent.

## Security

When exposing SemaDB to the internet, it is important to restrict access to it. Here are a few tips to secure your deployment:

- The HTTP API is often placed behind a reverse proxy such as [Nginx](https://www.nginx.com/) or [Traefik](https://traefik.io/) to handle SSL termination and rate limiting. In that case, you can use the `proxySecret` configuration to place a secret in the headers that only the reverse proxy knows.
- The HTTP API can also restricted to certain IP addresses using the `whiteListIPs` configuration. It is recommended to use this in conjunction with the proxy secret to ensure only the reverse proxy can access the API.
- The RPC API is assumed to be internal only and should not be publicly exposed. This is commonly achieved by placing the RPC communication on a private network or using a VPN.
- The metrics API can be exposed to [Prometheus](https://prometheus.io/) for scraping. It is again assumed to be internal only and should not be publicly exposed.