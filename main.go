package main

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"
)

var port = flag.Int("port", 1709, "http listening port")
var serveSql = flag.Bool("sql", false, "serve sql file instead of csv")

func main() {
	flag.Parse()

	if *port == 0 {
		fmt.Println("must supply --port")
		os.Exit(1)
	}

	httpSrv := getHttpServer(*port, *serveSql)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-quit
		signal.Stop(quit)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		fmt.Println("http server is shutting down")
		if err := httpSrv.Shutdown(ctx); err != nil {
			fmt.Println("failed to shutdown http server", err.Error())
		}
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		fmt.Println("Serving http on :", *port)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Println("Error serving http server:", err.Error())
		}
	}()

	wg.Wait()

}

func serveContents(w http.ResponseWriter, req *http.Request, contents *inMemContents, serveSql bool) {
	if req.Method != http.MethodPost {
		w.WriteHeader(http.StatusBadRequest)
		_, err := io.WriteString(w, "only POST requests supported.")
		if err != nil {
			fmt.Println(err.Error())
		}
		fmt.Println("received unsupported request method")
		return
	}

	fmt.Println("received request")

	// check for content-type
	contentType := req.Header.Get("Content-Type")
	if contentType == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Println("no request content-type header set")
		return
	}

	for k, v := range req.Header {
		fmt.Printf("request headers: %s: %v\n", k, v)
	}

	b := contents.ReadAll()

	fmt.Println("content-length:", contents.Len())
	fmt.Println("status-code:", http.StatusOK)
	fmt.Println()

	w.Header().Add("Content-Length", strconv.FormatInt(contents.Len(), 10))

	contentMd5, err := contents.getMd5()
	if err != nil {
		fmt.Println(err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if serveSql {
		writeSqlHeaders(w)
	} else {
		writeCsvHeaders(w)
	}

	// required for all
	w.Header().Add("X-Import-Md5", contentMd5)
	w.WriteHeader(http.StatusOK)

	n, err := w.Write(b)
	if err != nil {
		panic(err)
	}
	if int64(n) != contents.Len() {
		panic("failed to write all contents")
	}
}

func writeSqlHeaders(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "application/sql")
	w.Header().Add("X-Import-Filename", "transformed.sql")
}

func writeCsvHeaders(w http.ResponseWriter) {
	w.Header().Add("Content-Type", "text/csv")
	w.Header().Add("X-Import-Filename", "transformed.csv")
	w.Header().Add("X-Import-Table", "csv_table")
	w.Header().Add("X-Import-Operation", "overwrite")
	w.Header().Add("X-Import-Primary-Keys", "pk")
	w.Header().Add("X-Import-Primary-Keys", "col1")
}

type inMemContents struct {
	mu       *sync.Mutex
	contents []byte
}

var csvText = `pk,col1,col2,col3
1,a,b,c
2,d,e,f
3,g,h,i
`

var sqlText = `CALL DOLT_CHECKOUT('-b', 'import-branch-1');
CREATE TABLE t1 (
pk int primary key,
col1 varchar(55),
col2 varchar(55),
col3 varchar(55)
);
INSERT INTO t1 (pk, col1, col2, col3) VALUES (1, 'a', 'b', 'c');
INSERT INTO t1 (pk, col1, col2, col3) VALUES (2, 'd', 'e', 'f');
INSERT INTO t1 (pk, col1, col2, col3) VALUES (3, 'g', 'h', 'i');
CALL DOLT_COMMIT('-A', '-m', 'Create table t1');
CALL DOLT_CHECKOUT('main');
CALL DOLT_CHECKOUT('-b', 'import-branch-2');
CREATE TABLE t2 (
pk int primary key,
col1 varchar(55),
col2 varchar(55),
col3 varchar(55)    
);
INSERT INTO t2 (pk, col1, col2, col3) VALUES (1, 'j', 'k', 'l');
INSERT INTO t2 (pk, col1, col2, col3) VALUES (2, 'm', 'n', 'o');
INSERT INTO t2 (pk, col1, col2, col3) VALUES (3, 'p', 'q', 'r');
CALL DOLT_COMMIT('-A', '-m', 'Create table t2');
CALL DOLT_CHECKOUT('main');
CREATE TABLE t3 (
pk int primary key,
col1 varchar(55),
col2 varchar(55),
col3 varchar(55)    
);
INSERT INTO t3 (pk, col1, col2, col3) VALUES (1, 's', 't', 'u');
INSERT INTO t3 (pk, col1, col2, col3) VALUES (2, 'v', 'w', 'x');
INSERT INTO t3 (pk, col1, col2, col3) VALUES (3, 'y', 'z', 'aa');
`

func newCsvContents() *inMemContents {
	return &inMemContents{
		mu:       &sync.Mutex{},
		contents: []byte(csvText),
	}
}

func newSqlContents() *inMemContents {
	return &inMemContents{
		mu:       &sync.Mutex{},
		contents: []byte(sqlText),
	}
}

func (c *inMemContents) Len() int64 {
	return int64(len(c.contents))
}

func (c *inMemContents) ReadAll() []byte {
	return c.contents[:]
}

func (c *inMemContents) getMd5() (string, error) {
	r := bytes.NewReader(c.contents[:])
	hash := md5.New()
	_, err := io.Copy(hash, r)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

func getHttpServer(port int, serveSql bool) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		var contents *inMemContents
		if serveSql {
			contents = newSqlContents()
		} else {
			contents = newCsvContents()
		}
		serveContents(writer, request, contents, serveSql)
	})

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
}
