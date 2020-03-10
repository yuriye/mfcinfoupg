package mfcinfoupg

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"strconv"
	"strings"
)

func UpgradePositions(csvFName string, conn *pgx.Conn) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Id"]
	nameInd := fieldNames["Name"]
	arcInd := fieldNames["Archive"]
	//recs := make([]Position, 0, 50)
	var pos Position

	var name string
	var arc bool

	for _, rec := range arr[1:] {
		pos = Position{}
		pos.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			fmt.Println(err)
			continue
		}
		pos.name = strings.Trim(rec[nameInd], " ")
		if rec[arcInd] == "1" {
			pos.arc = true
		}

		err := conn.QueryRow(context.Background(),
			"select name, arc from positions where position_id = $1", pos.id).Scan(&name, &arc)
		if err != nil {
			if !pos.arc {
				_, err := conn.Exec(context.Background(),
					"insert into positions (position_id, name, arc) values ($1, $2, $3)",
					pos.id, pos.name, pos.arc)
				if err != nil {
					fmt.Println(err)
				}
			}
			continue
		}
		if name != pos.name || arc != pos.arc {
			_, err := conn.Exec(context.Background(),
				"UPDATE positions SET name = $2,  arc = $3 WHERE position_id = $1",
				pos.id, pos.name, pos.arc)
			if err != nil {
				fmt.Println(err)
			}
		}
		fmt.Printf("%s\t%b", name, arc)
		//recs = append(recs, pos)
	}
	//return recs, nil
}
