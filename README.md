## Finagle Consul

Service discovery for Finagle cluster with Consul. Multi DC, custom health checks.

### About

[Consul](https://www.consul.io/) is a tool for service discovery and configuration. Consul is distributed, highly available, and extremely scalable.

`finagle-consul` provides you Announcer and Resolver to work with low-level Consul HTTP API.

**Warning! This is still BETA.**

### QuickStart

Install and run Consul (see [docs](https://www.consul.io/intro/getting-started/install.html) for more information):

```shell
$ consul agent -server -bootstrap-expect 1 -data-dir /tmp/consul -ui-dir ~/Downloads/dist -log-level debug
```

Run one or more example servers (each one will be bound to the random socket):

```shell
$ sbt 'run-main com.twitter.finagle.consul.examples.Server'
[info] Running com.twitter.finagle.consul.examples.Server
Run server: sgN72ffoxX
Apr 08, 2015 10:45:58 PM com.twitter.finagle.consul.ConsulAnnouncer register
INFO: Register consul service ConsulService(3fc29cfa-75f6-4b59-8f2f-591c893a0f13,RandomNumber,192.168.0.104,57007)
```

You can now check list of registered services in [Consul Web UI](https://www.consul.io/intro/getting-started/ui.html).

![Consul Web UI](https://dl-web.dropbox.com/get/ConsulUI.png?_subject_uid=15709793&w=AADjbDUK1Ox7lL7aUbLOSAnW8eBl1McFGi4EG1mGOyQ61w)

Run example client:

```shell
$ sbt 'run-main com.twitter.finagle.consul.examples.Client'
[info] Running com.twitter.finagle.consul.examples.Client
Run CLIENT: 7spceT1R8f
Apr 08, 2015 10:48:12 PM com.twitter.finagle.consul.ConsulResolver$$anonfun$readCatalog$1 apply
INFO: Consul catalog lookup at localhost:8500 to look for RandomNumber: List(/192.168.0.104:57007, /192.168.0.104:57071)
Server:sgN72ffoxX; Client:7spceT1R8f; Req:50; Resp:p6Cli
Server:XmI4bk6khp; Client:7spceT1R8f; Req:47; Resp:p3lQb
Server:sgN72ffoxX; Client:7spceT1R8f; Req:42; Resp:ZJlzj
<...truncated output...>
```

### Consul path definition

TBD

### TODO

- [x] TTL configuration
- [x] Custom tags for Announcer
- [x] Multi DC configuration
- [ ] Tags filters for Resolver
- [ ] Package distribution
- [ ] Unit tests for consul query decode
- [ ] EndToEnd integration tests
- [ ] Debug level log messages for all Consul server queries
- [ ] Process Consul HTTP API errors/timeouts

(see also numerous "XXX" comments in code)

### Known issues

- "deregister" HTTP command works a bit unpredictably

- is any way to watch changes from HTTP API endpoint (?)

- registering service for unknown datacenter leads to silent error