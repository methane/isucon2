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
	artist, ticket, variation string
        artist_id, ticket_id, variation_id int
}

var (
	config = loadConfig()
	db     *sql.DB

	sellMutex   = &sync.RWMutex{}
	recentSold  []Data
	nextOrderId int
	stock       map[int][]SeatStock // artist_id -> seat

	//master
	variations map[int]Variation = make(map[int]Variation)
	artists    []Data
	tickets []Data
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
                    artist_id: aid, ticket_id:tid, variation_id: vid}
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
                    "id": id,
                    "name": name,
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
	go func() {
		db.Exec(`INSERT INTO order_request (id, member_id) VALUES (?, ?)`, orderId, memberId)
		db.Exec(`UPDATE stock SET order_id=? WHERE id=?`, orderId, seat.id)
	}()

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

var rowcol = make([]int, 64)


var ticketPageCache map[int]string = make(map[int]string)

func ticketPageMaker() {
    for {
        for i := 1; i<6; i++ {
            sellMutex.Lock()
            log.Println("Creating ticket page cache for", i)
            ticketId := i

            var (
                    tid, aid     int
                    tname, aname string
            )
            err := db.QueryRow("SELECT t.*, a.name AS artist_name FROM ticket t INNER JOIN artist a ON t.artist_id = a.id WHERE t.id = ? LIMIT 1", ticketId).Scan(&tid, &tname, &aid, &aname)
            if err != nil {
                    sellMutex.Unlock()
                    time.Sleep(time.Second)
                    continue;
            }

            ticket := Data{"Id": tid, "Name": tname, "ArtistId": aid, "ArtistName": aname}

            var rows *sql.Rows
            rows, err = db.Query("SELECT id, name FROM variation WHERE ticket_id = ?", tid)
            if err != nil {
                    sellMutex.Unlock()
                    time.Sleep(time.Second)
                    continue;
            }

            variations := []Data{}
            for rows.Next() {
                    var id int
                    var name string
                    if err := rows.Scan(&id, &name); err != nil {
                        sellMutex.Unlock()
                        time.Sleep(time.Second)
                        continue;
                    }

                    var rows2 *sql.Rows
                    rows2, err = db.Query("SELECT seat_id, order_id FROM stock WHERE variation_id = ?", id)
                    if err != nil {
                            sellMutex.Unlock()
                            time.Sleep(time.Second)
                            continue;
                    }
                    stock := make(Data)
                    for rows2.Next() {
                            var (
                                    seatId  string
                                    orderId interface{}
                            )
                            err = rows2.Scan(&seatId, &orderId)
                            if err != nil {
                                    log.Panic(err)
                            }

                            stock[seatId] = orderId
                    }
                    if err := rows2.Err(); err != nil {
                            log.Panic(err)
                    }

                    var vacancy int
                    err = db.QueryRow(`SELECT COUNT(*) AS cunt FROM stock WHERE variation_id = ? AND order_id IS NULL`, id).Scan(&vacancy)
                    if err != nil {
                            log.Panic(err)
                    }
                    variations = append(variations, Data{"Id": id, "Name": name, "Stock": stock, "Vacancy": vacancy})
            }
            if err := rows.Err(); err != nil {
                    log.Panic(err)
            }

            data := Data{
                    "Ticket":     ticket,
                    "Variations": variations,
                    "RecentSold": getRecentSold(),
                    "RowCol":     rowcol,
            }

            buf := bytes.Buffer{}
            ticketTmpl.ExecuteTemplate(&buf, "layout", data)
            ticketPageCache[ticketId] = buf.String()

            sellMutex.Unlock()
        }
        time.Sleep(time.Second / 10)
    }
}


func ticketHandler(w http.ResponseWriter, r *http.Request) {
	ticketId, err := getId(r.RequestURI)
	if err != nil {
		log.Panic(err)
	}

	cache, ok := ticketPageCache[ticketId]
	if ok {
		io.WriteString(w, cache)
		return
	}

	var (
		tid, aid     int
		tname, aname string
	)
	err = db.QueryRow("SELECT t.*, a.name AS artist_name FROM ticket t INNER JOIN artist a ON t.artist_id = a.id WHERE t.id = ? LIMIT 1", ticketId).Scan(&tid, &tname, &aid, &aname)
	if err != nil {
		log.Panic(err)
	}

	ticket := Data{"Id": tid, "Name": tname, "ArtistId": aid, "ArtistName": aname}

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, name FROM variation WHERE ticket_id = ?", tid)
	if err != nil {
		log.Panic(err)
	}

	variations := []Data{}
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			log.Panic(err)
		}

		var rows2 *sql.Rows
		rows2, err = db.Query("SELECT seat_id, order_id FROM stock WHERE variation_id = ?", id)
		if err != nil {
			log.Panic(err)
		}
		stock := make(Data)
		for rows2.Next() {
			var (
				seatId  string
				orderId interface{}
			)
			err = rows2.Scan(&seatId, &orderId)
			if err != nil {
				log.Panic(err)
			}

			stock[seatId] = orderId
		}
		if err := rows2.Err(); err != nil {
			log.Panic(err)
		}

		var vacancy int
		err = db.QueryRow(`
        SELECT COUNT(*) AS cunt FROM stock WHERE variation_id = ? AND
         order_id IS NULL`, id).Scan(&vacancy)
		if err != nil {
			log.Panic(err)
		}

		variations = append(variations, Data{"Id": id, "Name": name, "Stock": stock, "Vacancy": vacancy})
	}
	if err := rows.Err(); err != nil {
		log.Panic(err)
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
		log.Panic(err)
	}
	memberId := r.FormValue("member_id")

	orderId, seatId := sell(memberId, int(variationId))
	if orderId == 0 {
		completeTmpl.ExecuteTemplate(w, "layout", nil)
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

	for i, _ := range rowcol {
		rowcol[i] = i + 1
	}

	initMaster()
	initSellService()
        //go ticketPageMaker()

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
