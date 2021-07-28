package main

import (
	"bufio"
	"log"
	"os"
	"strings"
)

type cfgFile struct {
	c map[string][]string
}

var Cfg cfgFile = NewCfgFile()

func NewCfgFile() cfgFile {
	c := *new(cfgFile)
	c.c = make(map[string][]string)

	return c
}

func (t *cfgFile) Load(filename string) {

	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		str := strings.Trim(sc.Text(), " \t")
		if str == "" {
			continue
		}
		if str[0] == '#' {
			continue
		}

		a := strings.Split(str, "\t")
		if len(a) > 1 {
			t.c[strings.Trim(a[0], " \t")] = append(t.c[strings.Trim(a[0], " \t")], a[1])
		}
	}
	log.Printf("len(t.c)=%d", len(t.c))
}

func (t *cfgFile) GetValue(key string, def string) string {

	if _, ok := t.c[key]; ok {
		//do something here
		return t.c[key][0]
	}

	return def
}
