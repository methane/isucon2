package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"html/template"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"
)

type Config struct {
	Db *DbConfig `json:"database"`
}

type DbConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	UserName string `json:"username"`
	Password string `json:"password"`
	DbName   string `json:"dbname"`
}

func (db *DbConfig) String() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s",
		db.UserName,
		db.Password,
		db.Host,
		db.Port,
		db.DbName,
	)
}

func loadConfig() *Config {
	var c Config
	log.Println("Loading configuration")

	var env string
	if env = os.Getenv("ISUCON_ENV"); env == "" {
		env = "local"
	}

	if f, err := os.Open("../config/common." + env + ".json"); err == nil {
		defer f.Close()
		json.NewDecoder(f).Decode(&c)
	} else {
		panic(err)
	}

	return &c
}

func initDb() {
	log.Println("Initializing database")
	f, err := os.Open("../config/database/initial_data.sql")
	if err != nil {
		log.Panic(err.Error())
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		db.Exec(s.Text())
	}

	if err := s.Err(); err != nil {
		log.Panic(err.Error())
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
}

var (
	config = loadConfig()
	db     *sql.DB
)

var (
	indexTmpl    = parseTemplate("index")
	artistTmpl   = parseTemplate("artist")
	ticketTmpl   = parseTemplate("ticket")
	completeTmpl = parseTemplate("complete")
	soldoutTmpl  = parseTemplate("soldout")
	adminTmpl    = parseTemplate("admin")
)

var idRegexp = regexp.MustCompile(".+/([0-9]+)")

func getId(path string) (int, error) {
	id, err := strconv.ParseInt(idRegexp.FindStringSubmatch(path)[1], 10, 64)
	return int(id), err
}

func parseTemplate(name string) *template.Template {
	return template.Must(template.New(name).ParseFiles("templates/layout.html", "templates/"+name+".html"))
}

type Data map[string]interface{}

func getRecentSold() []Data {
	rows, err := db.Query(`
SELECT stock.seat_id, variation.name AS v_name, ticket.name AS t_name, artist.name AS a_name FROM stock
        JOIN variation ON stock.variation_id = variation.id
        JOIN ticket ON variation.ticket_id = ticket.id
        JOIN artist ON ticket.artist_id = artist.id
        WHERE order_id IS NOT NULL
        ORDER BY order_id DESC LIMIT 10
  `)
	if err != nil {
		log.Panic(err.Error())
	}

	var seatId, vName, tName, aName string
	solds := []Data{}
	for rows.Next() {
		if err := rows.Scan(&seatId, &vName, &tName, &aName); err != nil {
			log.Panic(err.Error())
		}
		data := Data{
			"SeatId":        seatId,
			"VariationName": vName,
			"TicketName":    tName,
			"AritistName":   aName,
		}
		solds = append(solds, data)
	}

	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	return solds
}

func topPageHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query("SELECT * FROM artist")
	if err != nil {
		log.Fatal(err)
	}

	artists := []Data{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		artists = append(artists, Data{"Id": id, "Name": name})
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	data := Data{
		"Artists":    artists,
		"RecentSold": getRecentSold(),
	}
	indexTmpl.ExecuteTemplate(w, "layout", data)
}

func artistHandler(w http.ResponseWriter, r *http.Request) {
	artistId, err := getId(r.RequestURI)
	if err != nil {
		log.Fatal(err)
	}

	var (
		aid   int
		aname string
	)
	err = db.QueryRow("SELECT id, name FROM artist WHERE id = ? LIMIT 1", artistId).Scan(&aid, &aname)

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, name FROM ticket WHERE artist_id = ?", artistId)
	if err != nil {
		log.Fatal(err)
	}

	tickets := []Data{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}
		var count int
		err = db.QueryRow(`
            SELECT COUNT(*) AS cnt FROM variation                     
            INNER JOIN stock ON stock.variation_id = variation.id    
            WHERE variation.ticket_id = ? AND stock.order_id IS NULL`,
			id).Scan(&count)
		tickets = append(tickets, Data{"Id": id, "Name": name, "Count": count})
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	data := Data{
		"Artist":     Data{"Id": aid, "Name": aname},
		"Tickets":    tickets,
		"RecentSold": getRecentSold(),
	}
	artistTmpl.ExecuteTemplate(w, "layout", data)
}

var rowcol = make([]int, 64)

func ticketHandler(w http.ResponseWriter, r *http.Request) {
	ticketId, err := getId(r.RequestURI)
	if err != nil {
		log.Fatal(err)
	}

	var (
		tid, aid     int
		tname, aname string
	)
	err = db.QueryRow("SELECT t.*, a.name AS artist_name FROM ticket t INNER JOIN artist a ON t.artist_id = a.id WHERE t.id = ? LIMIT 1", ticketId).Scan(&tid, &tname, &aid, &aname)
	if err != nil {
		log.Fatal(err)
	}

	ticket := Data{"Id": tid, "Name": tname, "ArtistId": aid, "ArtistName": aname}

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, name FROM variation WHERE ticket_id = ?", tid)
	if err != nil {
		log.Fatal(err)
	}

	variations := []Data{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Fatal(err)
		}

		var rows2 *sql.Rows
		rows2, err = db.Query("SELECT seat_id, order_id FROM stock WHERE variation_id = ?", id)
		if err != nil {
			log.Fatal(err)
		}
		stock := make(Data)
		for rows2.Next() {
			var (
				seatId  string
				orderId interface{}
			)
			err = rows2.Scan(&seatId, &orderId)
			if err != nil {
				log.Fatal(err)
			}

			stock[seatId] = orderId
		}
		if err := rows2.Err(); err != nil {
			log.Fatal(err)
		}

		var vacancy int
		err = db.QueryRow(`
        SELECT COUNT(*) AS cunt FROM stock WHERE variation_id = ? AND
         order_id IS NULL`, id).Scan(&vacancy)
		if err != nil {
			log.Fatal(err)
		}

		variations = append(variations, Data{"Id": id, "Name": name, "Stock": stock, "Vacancy": vacancy})
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}

	data := Data{
		"Ticket":     ticket,
		"Variations": variations,
		"RecentSold": getRecentSold(),
		"RowCol":     rowcol,
	}
	ticketTmpl.ExecuteTemplate(w, "layout", data)

}

func buyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		return
	}

	variationId, err := strconv.ParseInt(r.FormValue("variation_id"), 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	memberId := r.FormValue("member_id")

	var tx *sql.Tx
	tx, err = db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := recover(); err != nil {
			tx.Rollback()
			panic(err)
		}
	}()

	var result sql.Result
	result, err = tx.Exec("INSERT INTO order_request (member_id) VALUES (?)", memberId)
	if err != nil {
		log.Panic(err.Error())
	}

	orderId, err := result.LastInsertId()
	if err != nil {
		log.Panic(err.Error())
	}

	result, err = tx.Exec("UPDATE stock SET order_id = ? WHERE variation_id = ? AND order_id IS NULL ORDER BY RAND() LIMIT 1", orderId, variationId)

	var rows *sql.Rows
	rows, err = tx.Query("SELECT seat_id FROM stock WHERE order_id = ? LIMIT 1", orderId)
	if err != nil {
		log.Panic(err.Error())
	}

	if !rows.Next() {
		tx.Rollback()
		soldoutTmpl.ExecuteTemplate(w, "layout", nil)
		return
	}

	if err := rows.Err(); err != nil {
		log.Panic(err.Error())
	}

	var seatId string
	err = rows.Scan(&seatId)
	if err != nil {
		log.Panic(err.Error())
	}
	if err := rows.Close(); err != nil {
		log.Panic(err.Error())
	}

	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	data := Data{
		"MemberId": memberId,
		"SeatId":   seatId,
	}
	completeTmpl.ExecuteTemplate(w, "layout", data)
}

func adminHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		initDb()
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}

	adminTmpl.ExecuteTemplate(w, "layout", nil)
}

func adminCsvHandler(w http.ResponseWriter, r *http.Request) {
	rows, err := db.Query(`
        SELECT order_request.*, stock.seat_id, stock.variation_id, stock.updated_at                                                        
        FROM order_request JOIN stock ON order_request.id = stock.order_id
        ORDER BY order_request.id ASC`)
	if err != nil {
		log.Fatal(err)
	}

	orders := []Data{}
	var (
		oid      int
		memberId string
	)

	var (
		seatId      string
		variationId int
		updatedAt   time.Time
	)

	for rows.Next() {
		rows.Scan(&oid, &memberId, &seatId, &variationId, &updatedAt)
		order := Data{
			"Id":                oid,
			"MemberId":          memberId,
			"Stock.VariationId": variationId,
			"Stock.SeatId":      seatId,
			"Stock.UpdatedAt":   updatedAt,
		}
		orders = append(orders, order)
	}

	if err := rows.Err(); err != nil {
		log.Panic(err.Error())
	}

	wr := csv.NewWriter(w)
	for _, order := range orders {
		err := wr.Write([]string{
			fmt.Sprintf("%d", order["Id"]),
			fmt.Sprintf("%s", order["MemberId"]),
			fmt.Sprintf("%s", order["Stock.SeatId"]),
			fmt.Sprintf("%d", order["Stock.VariationId"]),
			fmt.Sprintf("%s", fmt.Sprintf("%s", order["Stock.UpdatedAt"])),
		})
		if err != nil {
			log.Panic(err.Error())
		}
	}

	wr.Flush()
	w.Header().Set("Content-type", "text/csv")
}

func main() {

	var err error
	db, err = sql.Open("mysql", config.Db.String())
	if err != nil {
		log.Panic(err.Error())
	}
	db.SetMaxIdleConns(256)

	for i, _ := range rowcol {
		rowcol[i] = i + 1
	}

	http.HandleFunc("/", topPageHandler)
	http.HandleFunc("/artist/", artistHandler)
	http.HandleFunc("/ticket/", ticketHandler)
	http.HandleFunc("/buy", buyHandler)
	http.HandleFunc("/admin", adminHandler)
	http.HandleFunc("/admin/order.csv", adminCsvHandler)

	http.Handle("/css/", http.FileServer(http.Dir("static")))
	http.Handle("/images/", http.FileServer(http.Dir("static")))
	http.Handle("/js/", http.FileServer(http.Dir("static")))
	http.Handle("/favicon.ico", http.FileServer(http.Dir("static")))
	log.Fatal(http.ListenAndServe(":5000", nil))
}
