package etl

import (
	"bytes"
	"crypto/des"
	"encoding/base64"
	"encoding/binary"
	"log"
	"net"
	"strings"
)

//由cookie里的BDUSS得到userid
func DecodeBDUSS(bduss string) uint64 {
	if len(bduss) != 192 {
		return 0
	}

	salt := 0

	for i := 0; i < 192; i++ {
		salt += i
		t, _ := binary.Uvarint([]byte(string(bduss[i])))
		salt += int(t)
	}

	salt = salt%2 + 1

	pos := 192 - salt
	bduss = bduss[pos:] + bduss[:pos]
	bduss = strings.Replace(strings.Replace(bduss, "-", "+", -1), "~", "/", -1)
	bstr, err := base64.StdEncoding.DecodeString(bduss)
	if err != nil {
		log.Println("base64 decode bduss error:", err)
	}
	var ret uint64
	b := make([]byte, 8)
	copy(b, bstr[68:72])
	r := bytes.NewReader(b)
	err = binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		log.Println("base64 encode uid error:", err)
	}

	return ret
}

func decode(baiduId []byte) int64 {
	if len(baiduId) != 32 {
		return -1
	}

	keyStr := "ZxdeacAD"
	block, err := des.NewCipher([]byte(keyStr))
	if err != nil {
		log.Println(err)
	}
	id := make([]byte, 16)
	des_cblock_size := 8
	output := make([]byte, des_cblock_size)
	inputEnc := make([]byte, des_cblock_size)
	var bcd []byte
	var asc []byte
	var pCurrentEnc []byte
	var pCurrentDec []byte

	for i := 0; i < 2; i++ {
		pCurrentEnc = baiduId[i*des_cblock_size*2:]
		pCurrentDec = id[i*des_cblock_size:]
		//fmt.Printf("pEnc %s\npDec %X\n", pCurrentEnc, pCurrentDec)
		bcd = inputEnc
		asc = pCurrentEnc

		for j := 0; j < des_cblock_size; j++ {
			bcd[j] = 0
		}
		for k := 0; k < des_cblock_size; k++ {
			asc_ := asc[k*2]
			var a = byte(0)
			var b = byte(0)
			if asc_ >= 'A' && asc_ <= 'F' {
				a = asc_ - 'A' + 10
			} else if asc_ >= 'a' && asc_ <= 'f' {
				a = asc_ - 'a' + 10
			} else {
				a = asc_ - '0'
			}

			asc_ = asc[k*2+1]

			if asc_ >= 'A' && asc_ <= 'F' {
				b = asc_ - 'A' + 10
			} else if asc_ >= 'a' && asc_ <= 'f' {
				b = asc_ - 'a' + 10
			} else {
				b = asc_ - '0'
			}

			bcd[k] = a*0x10 + b
		}

		//fmt.Printf("bcd %X\n", bcd)
		//fmt.Printf("in %X\n", inputEnc)
		block.Decrypt(output, inputEnc)
		//fmt.Printf("out %X\n", output)
		copy(pCurrentDec, output)
		//fmt.Printf("currdec %X\n", pCurrentDec)

		//fmt.Printf("id %X\n", id)
		_ = pCurrentDec
	}

	var ret int64
	b := make([]byte, 8)
	copy(b, id[4:8])
	r := bytes.NewReader(b)
	err = binary.Read(r, binary.LittleEndian, &ret)
	if err != nil {
		log.Println("decode timestamp error:", err)
	}
	return ret

}

func DecodeId(id string) int64 {
	return decode([]byte(id))
}

func MakeHiveMap(m map[string]string) string {
	i := 0
	var buf bytes.Buffer
	for k, v := range m {
		if i > 0 {
			buf.WriteByte(byte(2))
		}
		buf.WriteString(k)
		buf.WriteByte(byte(3))
		buf.WriteString(v)
		i++
	}
	return buf.String()
}

//ip转无符号整数
func ipToInt(ip string) (uint32, error) {
	ipo := net.ParseIP(ip)
	r := bytes.NewReader([]byte(ipo.To4()))
	var ipl uint32
	err := binary.Read(r, binary.BigEndian, &ipl)
	return ipl, err
}
