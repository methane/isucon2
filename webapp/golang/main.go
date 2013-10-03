package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"html/template"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func must(err error) {
	if err != nil {
		log.Panic(err)
	}
}

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
		log.Fatal(err)
	}

	return &c
}

func initDb() {
	log.Println("Initializing database")
	f, err := os.Open("../config/database/initial_data.sql")
	if err != nil {
		log.Fatal(err)
	}
	s := bufio.NewScanner(f)
	for s.Scan() {
		db.Exec(s.Text())
	}

	if err := s.Err(); err != nil {
		log.Panic(err.Error())
	}
}

var (
	db *sql.DB
	queryChan = make(chan string, 128)

	sellMutex  = &sync.RWMutex{}
	recentSold []Data
)

type Stock struct {
	Id   int
	Seat string
}

type Variation struct {
	Id       int
	Name     string
	TicketId int
	stock    []Stock
}

type Ticket struct {
	Id         int
	Name       string
	ArtistId   int
	variations []*Variation
}

type Artist struct {
	Id      int
	Name    string
	tickets []*Ticket
}

type Master struct {
	lock sync.RWMutex

	artists    map[int]*Artist
	tickets    map[int]*Ticket
	variations map[int]*Variation

	nextOrderId int
}

var M = Master{
	lock:        sync.RWMutex{},
	artists:     make(map[int]*Artist),
	tickets:     make(map[int]*Ticket),
	variations:  make(map[int]*Variation),
	nextOrderId: 0}

func loadArtists() {
	rows, err := db.Query(`select id, name from artist order by id`)
	must(err)
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		rows.Scan(&id, &name)
		M.artists[id] = &Artist{id, name, []*Ticket{}}
	}
}

func loadTickets() {
	rows, err := db.Query(`select id, name, artist_id from ticket order by id`)
	must(err)
	defer rows.Close()
	for rows.Next() {
		var id, aid int
		var name string
		rows.Scan(&id, &name, &aid)
		ticket := &Ticket{id, name, aid, []*Variation{}}
		M.tickets[id] = ticket
		M.artists[aid].tickets = append(M.artists[aid].tickets, ticket)
	}
}

func loadVariations() {
	rows, err := db.Query("SELECT id, name, ticket_id FROM variation")
	must(err)
	defer rows.Close()
	for rows.Next() {
		var id, tid int
		var name string
		rows.Scan(&id, &name, &tid)
		v := &Variation{id, name, tid, []Stock{}}
		M.variations[id] = v
		M.tickets[tid].variations = append(M.tickets[tid].variations, v)
	}
}

func loadStock() {
	rows, err := db.Query("SELECT id, variation_id, seat_id FROM stock WHERE order_id IS NULL ORDER BY RAND()")
	must(err)
	defer rows.Close()
	for rows.Next() {
		var id, vid int
		var seat string
		rows.Scan(&id, &vid, &seat)
		M.variations[vid].stock = append(
			M.variations[vid].stock,
			Stock{id, seat})
	}
}

func loadOrder() {
	maxid := 0
	rows, err := db.Query("SELECT id FROM order_request ORDER BY id desc LIMIT 10")
	must(err)

	for rows.Next() {
		var id int
		rows.Scan(&id)
		if maxid < id {
			maxid = id
		}
		//todo: recent sold
	}
	M.nextOrderId = maxid + 1
}

func initMaster() {
	loadArtists()
	loadTickets()
	loadVariations()
	loadStock()
	loadOrder()
}

func initSellService() {
	sellMutex.Lock()
	defer sellMutex.Unlock()

	initRecentSold()
	log.Println("nextOrderId", M.nextOrderId)
}

func sell(memberId string, variationId int) (orderId int, seatId string) {
	sellMutex.Lock()
	defer sellMutex.Unlock()

	orderId = M.nextOrderId
	M.nextOrderId++

	v := M.variations[variationId]
	if len(v.stock) == 0 {
		return 0, ""
	}
	stock := v.stock[len(v.stock)-1]
	v.stock = v.stock[:len(v.stock)-1]
	seatId = stock.Seat

	// 別 goroutineシーケンシャルにやる
	//db.Exec(`INSERT INTO order_request (id, member_id) VALUES (?, ?)`, orderId, memberId)
	//db.Exec(`UPDATE stock SET order_id=? WHERE id=?`, orderId, stock.Id)
	queryChan <- fmt.Sprintf("INSERT INTO order_request (id, member_id) VALUES (%d, '%s')", orderId, memberId)
	queryChan <- fmt.Sprintf("UPDATE stock SET order_id=%d WHERE id=%d", orderId, stock.Id)

	if len(recentSold) < 10 {
		recentSold = append(recentSold, nil)
	}
	copy(recentSold[1:], recentSold)

	ticket := M.tickets[v.TicketId]
	artist := M.artists[ticket.ArtistId]
	recentSold[0] = Data{
		"ArtistName":    artist.Name,
		"TicketName":    ticket.Name,
		"VariationName": v.Name,
		"SeatId":        stock.Seat}
	return
}

func parseTemplate(name string) *template.Template {
	return template.Must(template.New(name).ParseFiles("templates/layout.html", "templates/"+name+".html"))
}

var (
	indexTmpl    = parseTemplate("index")
	artistTmpl   = parseTemplate("artist")
	ticketTmpl   = parseTemplate("ticket")
	completeTmpl = parseTemplate("complete")
	soldoutTmpl  = parseTemplate("soldout")
	adminTmpl    = parseTemplate("admin")
)

func getId(path string) (int, error) {
	pos := strings.LastIndex(path, "/") + 1
	id, err := strconv.ParseInt(path[pos:], 10, 31)
	if err != nil {
		log.Println("Path doesn't finishes with id", path)
	}
	return int(id), err
}

type Data map[string]interface{}

func initRecentSold() []Data {
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
		log.Print(err)
	}
	rows.Close()
	recentSold = solds
	return solds
}

func getRecentSold() []Data {
	sellMutex.RLock()
	defer sellMutex.RUnlock()
	return recentSold
}

func topHandler(w http.ResponseWriter, r *http.Request) {
	//defer func(t time.Time) { log.Println("top", time.Now().Sub(t)) }(time.Now())
	artists := []*Artist{M.artists[1], M.artists[2]}
	data := Data{
		"Artists":    artists,
		"RecentSold": getRecentSold(),
	}
	indexTmpl.ExecuteTemplate(w, "layout", data)
}

var artistPageCache = make(map[int]string)
var artistPageExpire = make(map[int]int64)

func artistHandler(w http.ResponseWriter, r *http.Request) {
	//defer func(t time.Time) { log.Println("artist", time.Now().Sub(t)) }(time.Now())
	artistId, err := getId(r.RequestURI)
	if err != nil {
		log.Panic(err)
	}

	expire, ok := artistPageExpire[artistId]
	if ok && expire > time.Now().UnixNano() {
		io.WriteString(w, artistPageCache[artistId])
		return
	}

	artist := M.artists[artistId]

	tt := []Data{}
	for _, t := range artist.tickets {
		ss := 0
		for _, v := range t.variations {
			ss += len(v.stock)
		}
		tt = append(tt, Data{"Id": t.Id, "Name": t.Name, "Count": ss})
	}

	data := Data{
		"Artist":     artist,
		"Tickets":    tt,
		"RecentSold": getRecentSold(),
	}

	buff := bytes.Buffer{}
	artistTmpl.ExecuteTemplate(&buff, "layout", data)
	s := buff.String()
	io.WriteString(w, s)
	artistPageCache[artistId] = s
	artistPageExpire[artistId] = time.Now().UnixNano() + int64(time.Second)/2
}

type TicketPageCache struct {
	cache map[int]string
}

func (self *TicketPageCache) Worker() {
	for {
		for _, t := range M.tickets {
			self.cache[t.Id] = ticketPage(t.Id)
		}
		time.Sleep(time.Second / 2)
	}
}
func (self *TicketPageCache) Get(t int) string {
	cache := self.cache[t]
	if cache != "" {
		return cache
	}
	return ticketPage(t)
}

var ticketPageCache = TicketPageCache{cache: make(map[int]string)}

func ticketPage(ticketId int) string {
	ticket := M.tickets[ticketId]
	if ticket == nil {
		return ""
	}

	vv := []Data{}
	for _, variation := range ticket.variations {
		ss := make(map[string]bool)
		for _, seat := range variation.stock {
			ss[seat.Seat] = true
		}

		sb := bytes.Buffer{}
		for i := 0; i < 64; i++ {
			sb.WriteString("<tr>")
			for j := 0; j < 64; j++ {
				seat := fmt.Sprintf("%02d-%02d", i, j)
				sb.WriteString("<td id=\"")
				sb.WriteString(seat)
				sb.WriteString("\" class=\"")
				if ss[seat] {
					sb.WriteString("available")
				} else {
					sb.WriteString("unavailable")
				}
				sb.WriteString("\"></td>\n")
			}
			sb.WriteString("</tr>")
		}
		vv = append(vv, Data{
			"Id":      variation.Id,
			"Name":    variation.Name,
			"Seats":   template.HTML(sb.String()),
			"Vacancy": len(variation.stock)})
	}

	//ticket = Data{"Id": ticketId, "Name": v.ticket, "ArtistId": v.artist_id, "ArtistName": v.artist}
	tt := Data{
		"Id":         ticket.Id,
		"Name":       ticket.Name,
		"ArtistId":   ticket.ArtistId,
		"ArtistName": M.artists[ticket.ArtistId].Name}
	data := Data{
		"Ticket":     tt,
		"Variations": vv,
		"RecentSold": getRecentSold(),
	}

	buf := bytes.Buffer{}
	ticketTmpl.ExecuteTemplate(&buf, "layout", data)
	return buf.String()
}

func ticketHandler(w http.ResponseWriter, r *http.Request) {
	//defer func(t time.Time) { log.Println("ticket", time.Now().Sub(t)) }(time.Now())
	ticketId, err := getId(r.RequestURI)
	if err != nil {
		log.Panic(err)
	}
	page := ticketPageCache.Get(ticketId)
	io.WriteString(w, page)
}

func buyHandler(w http.ResponseWriter, r *http.Request) {
	//defer func(t time.Time) { log.Println("buy", time.Now().Sub(t)) }(time.Now())
	if r.Method != "POST" {
		return
	}

	variationId, err := strconv.ParseInt(r.FormValue("variation_id"), 10, 64)
	if err != nil {
		log.Panic(err)
	}
	memberId := r.FormValue("member_id")

	orderId, seatId := sell(memberId, int(variationId))
	if orderId == 0 {
		soldoutTmpl.ExecuteTemplate(w, "layout", nil)
		return
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
		initMaster()
		initSellService()
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	adminTmpl.ExecuteTemplate(w, "layout", nil)
}

func adminCsvHandler(w http.ResponseWriter, r *http.Request) {
	sellMutex.Lock()
	defer sellMutex.Unlock()
	time.Sleep(1)
	rows, err := db.Query(`
        SELECT order_request.*, stock.seat_id, stock.variation_id, stock.updated_at                                                        
        FROM order_request JOIN stock ON order_request.id = stock.order_id
        ORDER BY order_request.id ASC`)
	if err != nil {
		log.Panic(err)
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

	w.Header().Set("Content-type", "text/csv")
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
			log.Panic(err)
		}
	}
	wr.Flush()
}

func bgdb() {
	for q := range queryChan {
		db.Exec(q)
	}
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	config := loadConfig()
	var err error
	db, err = sql.Open("mysql", config.Db.String())
	if err != nil {
		log.Panic(err)
	}
	db.SetMaxIdleConns(256)

	initDb()
	initMaster()
	initSellService()
	go bgdb()
	go ticketPageCache.Worker()

	http.HandleFunc("/", topHandler)
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
