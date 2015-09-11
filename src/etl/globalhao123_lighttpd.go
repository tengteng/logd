package etl

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	net_url "net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type Url struct {
	str        string //整个url字符串
	url_path   string
	url_fields map[string]string
}

type Query struct {
	http_method   string
	url           Url
	http_protocol string
}

type Cookie struct {
	cookie_fields map[string]string
}

type LogLine struct {
	host      string
	ip        string
	time      string
	query     Query
	http_stat string
	length    string
	refer     string
	cookie    Cookie
	useragent string
	client_ip string
	others    string
}

type GlobalHao123 struct {
	spiders    []string
	dispatcher *Dispatcher

	outputChan  chan map[string]string
	inputChan   chan string
	inChanSize  int
	outChanSize int
	routineNum  int
	quitChan    chan bool //用于阻塞Wait的chan
}

func NewGlobalHao123(spiderFile string, inChanSize int, outChanSize int, routineNum int, d *Dispatcher) *GlobalHao123 {

	spiders := LoadSpiderList(spiderFile)

	och := make(chan map[string]string, outChanSize)
	ich := make(chan string, inChanSize)
	/*
	   dch := make(chan int, chanSize)
	   wch := make(chan int64, 1)
	*/
	quitChan := make(chan bool)
	return &GlobalHao123{spiders: spiders, dispatcher: d, outputChan: och, inputChan: ich, inChanSize: inChanSize, outChanSize: outChanSize, routineNum: routineNum, quitChan: quitChan}
}

func LoadSpiderList(fname string) []string {
	fin, err := os.Open(fname)
	defer fin.Close()
	spiders := make([]string, 0)
	if err != nil {
		log.Println("open file", fname, "error", err)
	} else {
		scanner := bufio.NewScanner(fin)
		for scanner.Scan() {
			line := scanner.Text()
			spiders = append(spiders, line)
		}
	}
	return spiders
}

//处理一行日志
func (g *GlobalHao123) processLine(line string) {
	res := g.splitLine(line)
	/*
	   log.Println("res begin")
	   for i, v := range res {
	       fmt.Println(i, v)
	   }

	   log.Println("res end")
	*/
	size := len(res)
	if size < 11 {
		log.Println("invalid line", line)
		return
	}
	//st := time.Now()
	q := g.parseRawQuery(res[3])
	ck := g.parseRawCookie(res[7 : size-3])
	//log.Println(ck)
	others := strings.Join(res[size-1:], " ")
	logLine := LogLine{host: res[0], ip: res[1], time: res[2], query: q, http_stat: res[4], length: res[5], refer: res[6], cookie: ck, useragent: res[size-3], client_ip: res[size-2], others: others}

	kvs := map[string]string{"event_product": "global_hao123", "event_action": "globalhao123open"} //event_action只有open会用

	g.simpleCopy(logLine, kvs)

	g.parseUrl(logLine.host, logLine.query.url, logLine.refer, kvs)

	g.parseCookie(logLine.cookie.cookie_fields, kvs)

	g.parseTime(logLine.time, kvs)

	g.parseSpider(logLine.useragent, kvs)

	g.parseIp(logLine.ip, logLine.client_ip, kvs)

	//log.Println("out chan", len(g.outputChan))
	g.outputChan <- kvs //送入channel
}

// parse 文件
func (g *GlobalHao123) ParseFile(fname string) error {
	fin, err := os.Open(fname)
	defer fin.Close()
	if err != nil {
		log.Println("open", fname, "error", err)
	} else {
		g.ParseReader(fin)
	}
	return err
}

//通用接口,处理各种reader
func (g *GlobalHao123) ParseReader(r io.Reader) {

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		//log.Println("in chan", len(g.inputChan))
		g.inputChan <- line
	}
	log.Println("etl reader finish")
}

//处理一行
func (g *GlobalHao123) ParseLine(line string) {
	g.inputChan <- line
}

func (g *GlobalHao123) Wait() {
	close(g.inputChan)
	<-g.quitChan
}

func (g *GlobalHao123) processRoutine(ch chan string, wg *sync.WaitGroup, routineId int) {
	log.Println("start parse routine", routineId)
	for line := range ch {
		g.processLine(line)
	}
	wg.Done() //inputChan关闭后，routine自动结束
	log.Println("close parse routine", routineId)
}

func (g *GlobalHao123) Start(forever bool) {
	quitCh := make(chan int)

	go g.dispatcher.Dispatch(g.outputChan, quitCh)

	wg := &sync.WaitGroup{}
	wg.Add(g.routineNum)
	for i := 0; i < g.routineNum; i++ {
		go g.processRoutine(g.inputChan, wg, i)
	}

	if forever {
		HandleQuitSignal()
	}
	wg.Wait()
	close(g.outputChan)
	<-quitCh //等待dispatcher quit
	log.Println("parse finish")
	g.quitChan <- true
}

//拆分每行各个部分
func (g *GlobalHao123) splitLine(line string) []string {
	l := len(line)
	partNum := 0
	start := 0
	newPart := true
	result := make([]string, 0)
	var left byte = 0
	var right byte = 0
	for i := 0; i < l; {
		char := line[i]
		if newPart && (char == '[' || char == '"') {
			left = char
			if left == '[' {
				right = ']'
			} else {
				right = '"'
			}
		}
		if char != ' ' || (right != 0 && char != right) {
			//避免匹配右边引号
			if start != i && char == right {
				left = 0
				right = 0 //匹配到右边结束符
			}

			i++
			newPart = false
			continue
		}
		if partNum != 2 && partNum != 3 && partNum != 8 {
			result = append(result, strings.Trim(line[start:i], "[]\""))
		}
		newPart = true
		partNum++
		i++
		start = i

	}
	if start < l-1 {
		result = append(result, strings.Trim(line[start:], "[]\""))
	}
	return result
}

func (g *GlobalHao123) parseRawQuery(queryStr string) Query {
	var query Query
	var url Url
	parts := strings.Split(queryStr, " ")
	nParts := len(parts)
	if nParts != 3 {
		log.Println("invalid query:", queryStr)
	}
	query.http_method = parts[0]
	part1 := ""
	if nParts > 1 {
		part1 = parts[1]
	}
	if nParts > 2 {
		query.http_protocol = parts[2]
	}

	tmp := strings.Split(part1, "?")
	url.str = part1
	url.url_path = tmp[0]
	url.url_fields = make(map[string]string)

	if len(tmp) > 1 {
		//拆分key value参数对
		kvs := strings.Split(tmp[1], "&")
		keyCount := make(map[string]int) //记录相同的参数的出现次数
		for _, kv := range kvs {
			pair := strings.Split(kv, "=")
			if len(pair) == 2 {
				k := strings.Trim(pair[0], "[]") //处理带方括号的参数
				_, ok := url.url_fields[k]
				if ok {
					origK := k
					k = fmt.Sprintf("%s%d", k, keyCount[origK])
					keyCount[origK]++
				} else {
					keyCount[k] = 1
				}

				decoded, err := net_url.QueryUnescape(pair[1])
				if err != nil {
					url.url_fields[k] = pair[1]
					log.Println("decode", pair[1], "error:", err)
				} else {
					url.url_fields[k] = strings.Trim(decoded, " ")
				}
			}
		}
	}
	query.url = url
	return query
}

func (g *GlobalHao123) parseRawCookie(cookies []string) Cookie {
	kvs := make(map[string]string)
	if !(len(cookies) == 1 && cookies[0] == "-") {
		for _, s := range cookies {
			s = strings.Trim(s, ";")
			pos := strings.Index(s, "=")
			if pos > 0 {
				kvs[s[0:pos]] = s[pos+1:]
			}
		}
	}
	return Cookie{kvs}
}

func (g *GlobalHao123) simpleCopy(logLine LogLine, kvs map[string]string) {
	kvs["globalhao123_host"] = logLine.host
	kvs["globalhao123_httpmethod"] = logLine.query.http_method
	kvs["globalhao123_httpversion"] = logLine.query.http_protocol
	kvs["event_httpstatus"] = logLine.http_stat
	kvs["event_referer"] = logLine.refer
	kvs["event_useragent"] = logLine.useragent
	kvs["event_others"] = logLine.others
}

func (g *GlobalHao123) parseUrl(host string, url Url, refer string, kvs map[string]string) {
	kvs["event_url"] = "http://" + host + url.str
	//默认值
	kvs["event_urlpath"] = ""
	kvs["event_urlparams"] = ""
	kvs["globalhao123_page"] = ""
	kvs["globalhao123_level"] = ""
	kvs["globalhao123_type"] = ""
	kvs["globalhao123click_sort"] = ""
	kvs["globalhao123click_position"] = ""
	kvs["globalhao123click_value"] = ""
	kvs["globalhao123click_url"] = ""
	kvs["globalhao123_tn"] = ""
	kvs["globalhao123_channel"] = ""
	kvs["globalhao123open_appid"] = ""

	u, err := net_url.Parse(kvs["event_url"])
	if err != nil {
		log.Println("parse event_url error:", err)
	} else {
		q := u.Query()
		kvs["event_urlpath"] = url.url_path
		kvs["event_urlparams"] = MakeHiveMap(url.url_fields)
		kvs["globalhao123_page"] = q.Get("page")
		kvs["globalhao123_level"] = q.Get("level")
		kvs["globalhao123_type"] = q.Get("type")
		kvs["globalhao123click_sort"] = q.Get("sort")
		kvs["globalhao123click_position"] = q.Get("position")
		kvs["globalhao123click_value"] = q.Get("value")
		kvs["globalhao123click_url"] = q.Get("url")
		kvs["globalhao123_tn"] = q.Get("tn")
		kvs["globalhao123_channel"] = q.Get("channel")
		kvs["globalhao123open_appid"] = q.Get("appid")
	}
	//parse tn from referer
	if tn, ok := kvs["globalhao123_tn"]; (!ok || tn == "" || tn == "/") && refer != "" {
		u, err = net_url.Parse(refer)
		if err != nil {
			log.Println("parse event_url from referer error:", err)
		} else {
			q := u.Query()
			tn := q.Get("tn")
			if tn == "" || tn == "%2F" {
				tn = "/"
			}
			kvs["globalhao123_tn"] = tn
		}

	}
}

//parse cookie, 主要parse baiduid
func (g *GlobalHao123) parseCookie(cookieFields map[string]string, kvs map[string]string) {
	j, err := json.Marshal(cookieFields)
	if err != nil {
		log.Println("marshal event_cookie error:", err)
	}
	kvs["event_cookie"] = string(j)
	tmp, _ := cookieFields["BAIDUID"]
	if len(tmp) < 32 {
		kvs["event_baiduid"] = ""
	} else {
		kvs["event_baiduid"] = tmp[:32]
	}
	tmp, _ = cookieFields["FLASHID"]
	if len(tmp) < 32 {
		kvs["globalhao123_flashid"] = tmp
	} else {
		kvs["globalhao123_flashid"] = tmp[:32]
	}
	tmp, _ = cookieFields["BDUSS"]
	tmp, _ = net_url.QueryUnescape(tmp) //BDUSS需要先unescape
	kvs["event_userid"] = fmt.Sprintf("%d", DecodeBDUSS(tmp))
	//从baiduid得到第一次访问的时间戳
	timeStamp := DecodeId(kvs["event_baiduid"])
	tm := time.Unix(timeStamp, 0)
	kvs["globalhao123_bdtime"] = tm.Format("2006-01-02 15:04:05")
	kvs["globalhao123_bddate"] = tm.Format("20060102")
	kvs["globalhao123_bdhour"] = tm.Format("15")
	kvs["globalhao123_bdminute"] = tm.Format("04")
	//从flashid得到时间戳
	timeStamp = DecodeId(kvs["globalhao123_flashid"])
	tm = time.Unix(timeStamp, 0)
	kvs["globalhao123_ftime"] = tm.Format("2006-01-02 15:04:05")
	kvs["globalhao123_fdate"] = tm.Format("20060102")
	kvs["globalhao123_fhour"] = tm.Format("15")
	kvs["globalhao123_fminute"] = tm.Format("04")
}

func (g *GlobalHao123) parseSpider(useragent string, kvs map[string]string) {
	kvs["event_isspider"] = "0"
	kvs["event_spiderdetail"] = ""

	for _, v := range g.spiders {
		if strings.Contains(useragent, v) {
			kvs["event_isspider"] = "1"
			kvs["event_spiderdetail"] = v
			break
		}
	}
}

func (g *GlobalHao123) parseTime(t string, kvs map[string]string) {
	tm, err := time.Parse("02/Jan/2006:15:04:05 -0700", t)
	if err != nil {
		log.Println("parse time error", err)
	} else {
		kvs["event_time"] = tm.Format("2006-01-02 15:04:05")
		kvs["event_day"] = tm.Format("20060102")
		kvs["event_hour"] = tm.Format("15")
	}
}

func (g *GlobalHao123) parseIp(ip string, client_ip string, kvs map[string]string) {
	vip := ip
	cip := client_ip
	parts := strings.Split(cip, "\"")
	lp := len(parts)
	if lp == 1 {
		cip = parts[0]
		if cip != "-" {
			vip = cip
		}
	} else if lp >= 2 {
		cip = strings.Split(parts[0], " ")[0]
		if cip != "-" {
			vip = cip
		}
	}

	kvs["event_ip"] = vip
	ipo := net.ParseIP(vip)
	r := bytes.NewReader([]byte(ipo.To4()))
	var ipl uint32
	err := binary.Read(r, binary.BigEndian, &ipl)
	if err != nil {
		log.Println("ip to long error:", err)
	}
	kvs["event_ipinlong"] = fmt.Sprintf("%d", ipl)
}
