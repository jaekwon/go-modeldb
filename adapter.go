/*
   This package is responsible for converting ? placeholders into
   $i placeholders, like MySQL to PostgesQL.
*/

package modeldb

import (
	"fmt"
	. "github.com/jaekwon/pego"
	"strings"
)

var phConversions = map[string]string{}

var phGrammar = Grm("S", map[string]*Pattern{
	"S": Seq(
		Ref("OTHER"),
		Seq(
			Ref("PH").Or(Ref("STR")),
			Ref("OTHER"),
		).Rep(0, -1),
	).Clist(),
	"OTHER": NegSet("'?").Rep(0, -1).Csimple(),
	"PH":    Char('?').Csimple(),
	"STR": Seq(
		Char('\''),
		Seq(
			Seq(Char('\\'), Any(1)).Or(
				NegSet("'")),
		).Rep(0, -1),
		Char('\''),
	).Csimple(),
})

func ReplacePH(items []interface{}) string {
	index := 1
	parts := []string{}
	for _, item := range items {
		if item == "?" {
			parts = append(parts, fmt.Sprintf("$%v", index))
			index++
		} else {
			parts = append(parts, item.(string))
		}
	}
	//fmt.Println(">> %v", strings.Join(parts, ""))
	return strings.Join(parts, "")
}

func ConvertPH(q string) string {
	if phConversions[q] != "" {
		return phConversions[q]
	}
	r, err, _ := Match(phGrammar, q)
	if err != nil {
		panic(err)
	}
	return ReplacePH(r.([]interface{}))
}
