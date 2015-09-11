package etl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

type columnsMap map[string]map[string][]string // {"type": {"columns":[] , "partitions":[]}, ...}

type FileSaver struct {
	typeColsMap   columnsMap
	writers       map[string]io.WriteCloser
	outDir        string
	outFilePrefix string //输出文件名的默认前缀
	mutex         *sync.RWMutex
}

func NewFileSaver(colsMapFile string, outDir string, outFilePrefix string) *FileSaver {
	m := loadColsMap(colsMapFile)
	w := make(map[string]io.WriteCloser)

	if outFilePrefix == "" {
		outFilePrefix = "etl"
	}
	mutex := &sync.RWMutex{}

	return &FileSaver{m, w, outDir, outFilePrefix, mutex}
}
func loadColsMap(fname string) columnsMap {
	m := make(columnsMap)
	content, err := ioutil.ReadFile(fname)
	if err != nil {
		log.Println("read cols map file error", err)
	} else {
		err = json.Unmarshal(content, &m)
		if err != nil {
			log.Println("decode cols map error", err)
		}
	}
	return m
}

func (this *FileSaver) writeFile(w io.WriteCloser, kvs map[string]string, kind string) {
	if w != nil {
		var ok bool
		cols, ok := this.typeColsMap[kind]["columns"]
		if !ok {
			log.Println("wrong log kind <", kind, "> when write file")
			return
		}

		var buf bytes.Buffer
		var val string
		nCols := len(cols)

		for i, col := range cols {
			val = ""
			if col != "" {
				val, ok = kvs[col]
				if !ok {
					log.Println(kind, "miss field:", col)
				}
			}
			//替换掉可能的换行符
			if val != "" {
				val = strings.Replace(val, "\n", "", -1)
			}
			buf.WriteString(val)
			if i != nCols-1 {
				buf.WriteByte('\t')
			}
		}
		if buf.Len() > 0 {
			buf.WriteByte('\n')
			w.Write(buf.Bytes())
		}
	}
}

func (this *FileSaver) makePartitionsPath(kvs map[string]string, partitions []string) string {
	tmp := make([]string, 0)
	for _, p := range partitions {
		v, ok := kvs[p]
		if !ok || v == "" {
			v = "NONE"
		}
		tmp = append(tmp, v)
	}
	return filepath.Join(tmp...)
}

func (this *FileSaver) Save(kvs map[string]string, kind string, routineId int) {
	//目录结构：/输出目录/四个大类型/分区构成的目录
	partitions, ok := this.typeColsMap[kind]["partitions"]
	if !ok {
		return
	}
	partitionPath := this.makePartitionsPath(kvs, partitions)
	path := filepath.Join(this.outDir, kind, partitionPath)
	filename := fmt.Sprintf("%s/%s_r%d", path, this.outFilePrefix, routineId)
	this.mutex.RLock()
	w, ok := this.writers[filename] //查找有无打开的文件操作符，可以避免一些系统调用
	this.mutex.RUnlock()
	if !ok {
		//先检查有无目录
		if _, err := os.Stat(path); err != nil && os.IsNotExist(err) {
			os.MkdirAll(path, 0775)
		}
		fout, err := os.OpenFile(filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		if err != nil {
			log.Println("open", filename, "file for write error", err)
			return
		} else {
			w = fout
			if w == nil {
				log.Println("[error] fout", filename, "is nil")
				return
			}
			this.mutex.Lock()
			this.writers[filename] = w
			this.mutex.Unlock()
		}
	}
	this.writeFile(w, kvs, kind)
}

//清理过期的文件操作符
func (this *FileSaver) CloseWriters(all bool) {
	now := time.Now().Unix()
	var interval = int64(5 * 86400)

	re, _ := regexp.Compile(`/(\d{8})/`)
	for k, w := range this.writers {
		if all {
			w.Close()
			this.mutex.Lock()
			delete(this.writers, k)
			this.mutex.Unlock()
			continue
		}

		ret := re.FindSubmatch([]byte(k))
		if ret == nil {
			//格式不对，直接关闭
			w.Close()
			this.mutex.Lock()
			delete(this.writers, k)
			this.mutex.Unlock()
			log.Println("[wrong writer]", k)
		} else {
			date := string(ret[1])
			tm, err := time.Parse("20060102", date)
			if err != nil || (now-tm.Unix()) > interval {
				w.Close()
				this.mutex.Lock()
				delete(this.writers, k)
				this.mutex.Unlock()
				log.Println("[clear writer]", k)
			}
		}
	}
}
