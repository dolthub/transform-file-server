# transform-file-server
simple example of a file upload transform server

To run:
```bash
go run .
Serving http on : 1709
```

This server will respond to Transform `POST` requests with example CSV content. If run with `--sql`, will respond with SQL content that will create multiple branches.

Server logs are like:

```
received request
request headers: X-Import-Filename: [states.csv]
request headers: User-Agent: [Go-http-client/1.1]
request headers: X-Dolthub-Database: [example-database]
request headers: X-Dolthub-From-Branch: [example-user/import-fashionable-wolf]
request headers: X-Dolthub-Owner: [example-user]
request headers: X-Dolthub-To-Branch: [main]
request headers: Content-Type: [text/csv]
request headers: X-Import-Md5: [UjtXMOXuBEBXKXh4tkmvhQ==]
request headers: Accept-Encoding: [gzip]
content-length: 42
status-code: 200
```
