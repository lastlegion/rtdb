# rtdb

Real time databases with CLI, only supports `GET` and `PUT` operations.

### Dependencies 
Start redis, and build project binary
```
docker-compose up
go build
```


### Quickstart 
In 2 seperate terminal windows 

```
❯ ./rtdb --client_id=xyz
Real time db
> PUT test_key test_val
Done...
> 
```

```
❯ ./rtdb --client_id=abc
Real time db
> GET test_key
test_val
>
```
