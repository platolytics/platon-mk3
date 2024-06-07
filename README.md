# Platon Mk3

Blog: platon-mk3.org

This project is currently WIP. The Mk3 stack consists of:

* Platon (queries prometheus and updates metrics in SQL format)
* ClickHouse (columnstore database holding metrics for analysis)
* Superset (Analytics UI supporting ClickHouse)

## Get started

To get started with the current WIP state, connecting Platon to the Prometheus of an OpenShift cluster:

### Build and run dependent containers

```
$ make run-deps
```

### Build Platon

```
$ make
```

### Port-forward Prometheus

```
$ oc port-forward pod/prometheus-k8s-0 9091:9090 -n openshift-monitoring
```

### Run Platon

```
$ ./platon run -p http://localhost:9091 -c apiserver-cube.yaml
```

### Open Superset

Superset should be available at localhost:8080.
To view the data of the apiserver cube, create a datasource and chart from platon_db.default.apiservermemory
