package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"isucon2"
	"log"
	"net/http"
)

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

func main() {
	isucon2.Conf = isucon2.LoadConfig("../config/")

	// TODO: $B$3$l$8$c$?$V$s%@%a(B
	var err error
	isucon2.Db, err = sql.Open("mysql", isucon2.Conf.Db.String())
	if err != nil {
		log.Panic(err.Error())
	}
	defer isucon2.Db.Close()

	http.HandleFunc("/", isucon2.TopPageHandler)
	http.Handle("/css/", http.FileServer(http.Dir("static")))
	http.Handle("/images/", http.FileServer(http.Dir("static")))
	http.Handle("/js/", http.FileServer(http.Dir("static")))
	http.Handle("/favicon.ico", http.FileServer(http.Dir("static")))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
