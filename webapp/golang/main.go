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
	"math/rand"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
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

type TicketPageCache struct {
	cache map[int]string
}

func (self *TicketPageCache) Worker() {
	for {
		for p := 1; p < 11; p++ {
			self.cache[p] = ticketPage(p)
		}
		time.Sleep(time.Second / 4)
	}
}

func (self *TicketPageCache) Get(t int) string {
	cache, ok := self.cache[t]
	if ok {
		return cache
	}
	return ticketPage(t)
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

type SeatStock struct {
	id   int
	seat string
}

type Variation struct {
	artist, ticket, variation          string
	artist_id, ticket_id, variation_id int
}

var (
	config = loadConfig()
	db     *sql.DB

	sellMutex   = &sync.RWMutex{}
	recentSold  []Data
	nextOrderId int
	stock       map[int][]SeatStock // variation_id -> seat

	//master
	variations map[int]Variation = make(map[int]Variation)
	artists    []Data
	tickets    []Data
)

func initMaster() {
	rows, err := db.Query(`select artist.id, ticket.id, variation.id, artist.name, ticket.name, variation.name
            from artist, ticket, variation WHERE artist.id = ticket.artist_id AND ticket.id = variation.ticket_id`)

	if err != nil {
		log.Panic(err)
	}

	for rows.Next() {
		var aid, tid, vid int
		var artist, ticket, variation string

		rows.Scan(&aid, &tid, &vid, &artist, &ticket, &variation)
		variations[vid] = Variation{artist: artist, ticket: ticket, variation: variation,
			artist_id: aid, ticket_id: tid, variation_id: vid}
	}
	rows.Close()

	rows, err = db.Query("SELECT * FROM artist")
	if err != nil {
		log.Print(err)
	}

	artists = []Data{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Panic(err)
		}
		artists = append(artists, Data{"Id": id, "Name": name})
	}
	rows.Close()

	rows, err = db.Query("SELECT id, name, artist_id FROM ticket")
	if err != nil {
		log.Panic(err)
	}

	tickets = []Data{}
	for rows.Next() {
		var id, aid int
		var name string
		if err := rows.Scan(&id, &name, &aid); err != nil {
			log.Panic(err)
		}
		tickets = append(tickets, Data{
			"id":        id,
			"name":      name,
			"artist_id": aid,
		})
	}
	rows.Close()
}

func initSellService() {
	sellMutex.Lock()
	defer sellMutex.Unlock()

	initRecentSold()

	rows, err := db.Query(`SELECT MAX(id) FROM order_request`)
	if err != nil {
		log.Panic(err)
	}
	rows.Next()
	var max_id int
	rows.Scan(&max_id)
	rows.Close()
	nextOrderId = max_id + 1

	log.Println("nextOrderId", nextOrderId)

	if rows, err = db.Query(`SELECT id, variation_id, seat_id FROM stock WHERE order_id IS NULL`); err != nil {
		log.Panic(err)
	}

	// stock をメモリ上に確保しておく.
	stock = make(map[int][]SeatStock)
	for rows.Next() {
		var id, variationId int
		var seatId string
		rows.Scan(&id, &variationId, &seatId)
		stock[variationId] = append(stock[variationId], SeatStock{id: id, seat: seatId})
	}
	rows.Close()

	for _, seats := range stock {
		L := len(seats)
		for i := 0; i < L; i++ {
			r := rand.Int31n(int32(L))
			seats[i], seats[r] = seats[r], seats[i]
		}
	}
}

func sell(memberId string, variationId int) (orderId int, seatId string) {
	sellMutex.Lock()
	defer sellMutex.Unlock()

	if len(stock[variationId]) == 0 {
		return 0, ""
	}

	orderId = nextOrderId
	nextOrderId++

	var seat SeatStock
	ss := stock[variationId]
	seat, stock[variationId] = ss[len(ss)-1], ss[:len(ss)-1]

	// 別 goroutineシーケンシャルにやる
	db.Exec(`INSERT INTO order_request (id, member_id) VALUES (?, ?)`, orderId, memberId)
	db.Exec(`UPDATE stock SET order_id=? WHERE id=?`, orderId, seat.id)

	seatId = seat.seat
	vari := variations[variationId]

	if len(recentSold) < 10 {
		recentSold = append(recentSold, nil)
	}
	copy(recentSold[1:], recentSold)
	recentSold[0] = Data{
		"ArtistName":    vari.artist,
		"TicketName":    vari.ticket,
		"VariationName": vari.variation,
		"SeatId":        seatId}
	return
}

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

func topPageHandler(w http.ResponseWriter, r *http.Request) {
	data := Data{
		"Artists":    artists,
		"RecentSold": getRecentSold(),
	}
	indexTmpl.ExecuteTemplate(w, "layout", data)
}

func artistHandler(w http.ResponseWriter, r *http.Request) {
	artistId, err := getId(r.RequestURI)
	if err != nil {
		log.Panic(err)
	}

	var artist Data
	for _, artist = range artists {
		if artist["ArtistId"] == artistId {
			break
		}
	}

	var (
		aid   int
		aname string
	)
	//err = db.QueryRow("SELECT id, name FROM artist WHERE id = ? LIMIT 1", artistId).Scan(&aid, &aname)
	aid = artistId
	if aid == 1 {
		aname = "NHN48"
	} else {
		aname = "はだいろクローバーZ"
	}

	tt := []Data{}
	for _, t := range tickets {
		if t["artist_id"] != aid {
			continue
		}
		tid, _ := t["id"].(int)
		tname, _ := t["name"].(string)
		ss := 0

		for vid, v := range variations {
			if v.ticket_id == tid {
				ss += len(stock[vid])
			}
		}
		tt = append(tt, Data{"Id": tid, "Name": tname, "Count": ss})
	}

	data := Data{
		"Artist":     Data{"Id": aid, "Name": aname},
		"Tickets":    tt,
		"RecentSold": getRecentSold(),
	}
	artistTmpl.ExecuteTemplate(w, "layout", data)
}

var ticketPageCache = TicketPageCache{ cache: make(map[int]string)}

func ticketPage(ticketId int) string {
	var ticket Data
	for _, v := range variations {
		if v.ticket_id == ticketId {
			ticket = Data{"Id": ticketId, "Name": v.ticket, "ArtistId": v.artist_id, "ArtistName": v.artist}
			break
		}
	}

	vv := []Data{}
	for i := 1; i <= 11; i++ {
		v := variations[i]
		if v.ticket_id != ticketId {
			continue
		}
		vacancy := len(stock[v.variation_id])

		ss := make(map[string]bool)
		for _, seat := range stock[v.variation_id] {
			ss[seat.seat] = true
		}

		sb := bytes.Buffer{}
		for i := 0; i < 64; i++ {
			sb.WriteString("<tr>")
			for j := 0; j < 64; j++ {
				seat := fmt.Sprintf("%02d-%02d", i, j)
				sb.WriteString("<td id=\"")
				sb.WriteString(seat)
				sb.WriteString("\" class=\"")
				if _, ok := ss[seat]; ok {
					sb.WriteString("available")
				} else {
					sb.WriteString("unavailable")
				}
				sb.WriteString("\"></td>\n")
			}
			sb.WriteString("</tr>")
		}
		vv = append(vv, Data{
			"Id": v.variation_id, "Name": v.variation,
			"Seats": template.HTML(sb.String()), "Vacancy": vacancy})
	}

	data := Data{
		"Ticket":     ticket,
		"Variations": vv,
		"RecentSold": getRecentSold(),
	}

	buf := bytes.Buffer{}
	ticketTmpl.ExecuteTemplate(&buf, "layout", data)
	return buf.String()
}

func ticketHandler(w http.ResponseWriter, r *http.Request) {
	ticketId, err := getId(r.RequestURI)
	if err != nil {
		log.Panic(err)
	}
	time.Sleep(time.Second / 10)  // XXX
	page := ticketPageCache.Get(ticketId)
	io.WriteString(w, page)
}

func buyHandler(w http.ResponseWriter, r *http.Request) {
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

	initMaster()
	initSellService()
	go ticketPageCache.Worker()

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
