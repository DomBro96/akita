package common

import (
	"bufio"
	"io"
	"os"
	"strings"
)

const (
	sep  = "."
)

type Config struct {
	ConfMap map[string]string
	section string
}

func (c *Config) InitConfig(path string) error  {
	c.ConfMap = make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		s := strings.TrimSpace(string(b))
		if strings.Index(s, "#") == 0 {
			continue
		}
		n1 := strings.Index(s, "[")
		n2 := strings.LastIndex(s, "]")
		if n1 > -1 && n2 > -1 && n2 > n1 + 1 {
			c.section = strings.TrimSpace(s[n1+1:n2])
			continue
		}

		eqIndex := strings.Index(s, "=")
		if eqIndex < 0 {
			continue
		}
		key := strings.TrimSpace(s[:eqIndex])
		if len(key) == 0 {
			continue
		}
		val := strings.TrimSpace(s[eqIndex + 1:])

		valPos := strings.Index(val, "\t#")
		if valPos > -1 {
			val = val[0:valPos]
		}

		valPos = strings.Index(val, " #")
		if valPos > -1 {
			val = val[0:valPos]
		}

		valPos = strings.Index(val, "\t//")
		if valPos > -1 {
			val = val[0:valPos]
		}

		valPos = strings.Index(val, " //")
		if valPos > -1 {
			val = val[0:valPos]
		}

		if len(val) == 0 {
			continue
		}

		section := ""
		if c.section != "" {
			section += c.section + sep
		}
		section += key
		c.ConfMap[section] = strings.TrimSpace(val)
	}
	return nil
}

func (c Config) Read(key string) string {
	v, ok := c.ConfMap[key]
	if !ok {
		return ""
	}
	return v
}
