Start a docker container

```
docker run -d -e "NAMESPACE=test" --name aerospike -p 3000-3002:3000-3002 -v aerospike-etc-6-1:/opt/aerospike/etc/ -v aerospike-data-6-1:/opt/aerospike/data aerospike:ce-6.1.0.3
```

And run the complete Unit test on the class ExpressionsTest1
