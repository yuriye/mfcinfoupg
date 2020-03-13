package mfcinfoupg

import (
	"context"
	"fmt"
	"github.com/jackc/pgx"
	"log"
	"strconv"
	"strings"
	"time"
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
	}
}

func UpgradeDivisions(csvFName string, conn *pgx.Conn) {
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
	var div Division
	for _, rec := range arr[1:] {
		div = Division{}
		div.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			log.Println(err)
			continue
		}
		div.name = strings.Trim(rec[nameInd], " ")
		if rec[arcInd] == "1" {
			div.arc = true
		}
		var name string
		var arc bool
		err := conn.QueryRow(context.Background(),
			"select name, arc from divisions where division_id = $1", div.id).Scan(&name, &arc)
		if err != nil {
			if !div.arc {
				_, err := conn.Exec(context.Background(),
					"insert into divisions (division_id, name, arc) values ($1, $2, $3)",
					div.id, div.name, div.arc)
				if err != nil {
					log.Println(err)
				}
			}
			continue
		}
		if name != div.name || arc != div.arc {
			_, err := conn.Exec(context.Background(),
				"UPDATE divisions SET name = $2,  arc = $3 WHERE division_id = $1",
				div.id, div.name, div.arc)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func UpgradePeople(csvFName string, conn *pgx.Conn) {
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
	surnameInd := fieldNames["Name"]
	firstnameInd := fieldNames["Fname"]
	patronymicInd := fieldNames["Lname"]
	arcInd := fieldNames["Archive"]
	var hum Human
	for _, rec := range arr[1:] {
		hum = Human{}
		hum.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			log.Println(err)
			continue
		}
		hum.surname = strings.Trim(rec[surnameInd], " ")
		hum.firstname = strings.Trim(rec[firstnameInd], " ")
		hum.patronymic = strings.Trim(rec[patronymicInd], " ")
		csvname := hum.surname + " " + hum.firstname + " " + hum.patronymic
		if rec[arcInd] == "1" {
			hum.arc = true
		}
		var name string
		var arc bool
		err := conn.QueryRow(context.Background(),
			"select name, arc from people where human_id = $1", hum.id).Scan(&name, &arc)
		if err != nil {
			if !hum.arc {
				_, err := conn.Exec(context.Background(),
					"insert into people (human_id, name, arc) values ($1, $2, $3)",
					hum.id, csvname, hum.arc)
				if err != nil {
					log.Println(err)
				}
			}
			continue
		}
		if name != csvname || arc != hum.arc {
			_, err := conn.Exec(context.Background(),
				"UPDATE people SET name = $2, arc = $3 WHERE human_id = $1",
				hum.id, csvname, hum.arc)
			if err != nil {
				log.Println(err)
			}
		}
	}
}

func UpgradeStaff(csvFName string, conn *pgx.Conn) {
	var humanId, tabNomer, positionId, divisionId int
	var arc bool

	inStaff, _ := GetStaff(csvFName)
	for _, emp := range inStaff {
		err := conn.QueryRow(context.Background(),
			"select human_id, tabnomer, position_id, division_id, arc from staff where employee_id = $1", emp.id).
			Scan(&humanId, &tabNomer, &positionId, &divisionId, &arc)
		if err != nil {
			if !emp.arc {
				_, err := conn.Exec(context.Background(),
					"insert into staff (employee_id, human_id, tabnomer, position_id, division_id, arc) values ($1, $2, $3, $4, $5, $6)",
					emp.id, emp.humanId, emp.tabNomer, emp.positionId, emp.divisionId, emp.arc)
				if err != nil {
					log.Println(err)
				}
			}
			continue
		}
		if emp.humanId != humanId || emp.positionId != positionId ||
			emp.tabNomer != tabNomer || emp.divisionId != divisionId {
			_, err := conn.Exec(context.Background(),
				"UPDATE staff SET human_id = $2, tabnomer = $3, position_id = $4, division_id = $5, arc = $6 WHERE employee_id = $1",
				emp.id, emp.humanId, emp.tabNomer, emp.positionId, emp.divisionId, emp.arc)
			if err != nil {
				log.Println(err)
			}
		}

	}
}

func UpgradeRelocations(csvFName string, conn *pgx.Conn) {
	var employeeId, relocationtypeId, positionId, divisionId int
	var date, datee, dated time.Time

	inRelocs, _ := GetRelocations(csvFName)
	for _, reln := range inRelocs {
		err := conn.QueryRow(context.Background(),
			"SELECT employee_id, relocationtype_id, position_id, division_id, date, datee, dated FROM relocations WHERE relocation_id = $1", reln.id).
			Scan(&employeeId, &relocationtypeId, &positionId, &divisionId, &date, &datee, &dated)
		if err != nil {
			fmt.Println(err)
			_, err := conn.Exec(context.Background(),
				"insert into relocations (relocation_id, employee_id, relocationtype_id, position_id, division_id, date, datee, dated) values ($1, $2, $3, $4, $5, $6, $7, $8)",
				reln.id, reln.employeeId, reln.relocationTypeId, reln.positionId, reln.divisionId, reln.date, reln.dateE, reln.dateD)
			if err != nil {
				log.Println(err)
			}
			continue
		}
		if reln.employeeId != employeeId || reln.relocationTypeId != relocationtypeId || reln.positionId != positionId ||
			reln.divisionId != divisionId || reln.date != date || reln.dateE != datee || reln.dateD != dated {
			_, err := conn.Exec(context.Background(),
				"UPDATE relocations SET employee_id = $2, relocationtype_id = $3, position_id = $4, division_id = $5, date = $6, "+
					"datee = $7, dated = $8 WHERE relocation_id = $1",
				reln.id, reln.employeeId, reln.relocationTypeId, reln.positionId, reln.divisionId, reln.date, reln.dateE, reln.dateD)
			if err != nil {
				log.Println(err)
			}
		}
	}
}
