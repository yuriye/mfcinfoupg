package mfcinfoupg

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v4"
)

func UpgradePositions(csvFName string, conn *pgx.Conn) {
	var name string
	var arc bool
	positions, _ := GetPositions(csvFName)
	for _, pos := range positions {
		err := conn.QueryRow(context.Background(),
			"SELECT name, arc FROM positions WHERE position_id = $1", pos.id).Scan(&name, &arc)
		if err != nil {
			if !pos.arc {
				_, err := conn.Exec(context.Background(),
					"INSERT INTO positions (position_id, name, arc) VALUES ($1, $2, $3)",
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
	var name string
	var arc bool
	divisions, _ := GetDivisions(csvFName)
	for _, div := range divisions {
		err := conn.QueryRow(context.Background(),
			"SELECT name, arc FROM divisions WHERE division_id = $1", div.id).Scan(&name, &arc)
		if err != nil {
			if !div.arc {
				_, err := conn.Exec(context.Background(),
					"INSERT INTO divisions (division_id, name, arc) VALUES ($1, $2, $3)",
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
	var name string
	var gender, arc bool
	people, _ := GetPeople(csvFName)
	for _, hum := range people {
		fullname := hum.surname + " " + hum.firstname + " " + hum.patronymic
		err := conn.QueryRow(context.Background(),
			"SELECT name, gender, arc FROM people WHERE human_id = $1", hum.id).Scan(&name, &gender, &arc)
		if err != nil {
			if !hum.arc {
				_, err := conn.Exec(context.Background(),
					"INSERT INTO people (human_id, name, gender, arc) VALUES ($1, $2, $3, $4)",
					hum.id, fullname, hum.gender, hum.arc)
				if err != nil {
					log.Println(err)
				}
			}
			continue
		}

		if name != fullname || gender != hum.gender || arc != hum.arc {
			_, err := conn.Exec(context.Background(),
				"UPDATE people SET name = $2, gender = $3, arc = $4 WHERE human_id = $1",
				hum.id, fullname, hum.gender, hum.arc)
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
			"SELECT human_id, tabnomer, position_id, division_id, arc FROM staff WHERE employee_id = $1", emp.id).
			Scan(&humanId, &tabNomer, &positionId, &divisionId, &arc)
		if err != nil {
			if !emp.arc {
				_, err := conn.Exec(context.Background(),
					"insert into staff (employee_id, human_id, tabnomer, position_id, division_id, arc) VALUES ($1, $2, $3, $4, $5, $6)",
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

func UpgradeVacations(csvFName string, conn *pgx.Conn) {
	var staffIds = map[int]int{}
	rows, err := conn.Query(context.Background(), "SELECT employee_id tabnomer FROM staff")
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()

	for rows.Next() {
		var employeeId, tabNomer int
		rows.Scan(&employeeId, &tabNomer)
		staffIds[tabNomer] = employeeId
	}

	var vacationId, employeeId, days int
	var startDate, endDate time.Time

	inVacations, _ := GetVacations(csvFName)

	for _, vacation := range inVacations {

		err := conn.QueryRow(context.Background(),
			"SELECT vacation_id, employee_id, date_start, date_end, days FROM vacations WHERE vacation_id = $1", vacation.id).
			Scan(&vacationId, &employeeId, &startDate, &endDate, &days)
		if err != nil {
			_, err := conn.Exec(context.Background(),
				"insert into vacations (vacation_id, employee_id, date_sart, date_end, days) VALUES ($1, $2, $3, $4, $5)",
				vacation.id, vacation.employeeId, vacation.dateStart, vacation.dateEnd, vacation.days)
			if err != nil {
				log.Println(err)
			}
			continue
		}

		if vacation.employeeId != employeeId || vacation.dateStart != startDate ||
			vacation.dateEnd != endDate || vacation.days != days {
			_, err := conn.Exec(context.Background(),
				"UPDATE vacations SET vacation_id = $1, employee_id = $2, date_sart = $3, date_end = $4, days = $5",
				vacation.id, vacation.employeeId, vacation.dateStart, vacation.dateEnd, vacation.days)
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
				"INSERT INTO relocations (relocation_id, employee_id, relocationtype_id, position_id, division_id, date, datee, dated) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
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

func UpgradeTimesheets(csvFName string, conn *pgx.Conn) {
	timesheets, _ := GetTimesheets(csvFName, time.Now().Year())
	var timesheetId, employeeId, daystotal int
	var yearMonth time.Time
	var hourstotal float64
	var md5 string

	for _, tms := range timesheets {
		if err := conn.QueryRow(context.Background(),
			"SELECT timesheet_id, employee_id, year_month, daystotal, hourstotal, md5 FROM timesheets WHERE employee_id = $1 AND yearMonth = $2",
			tms.employeeId, tms.month).
			Scan(&timesheetId, &employeeId, &yearMonth, &daystotal, &hourstotal, &md5); err != nil {
			_, err := conn.Exec(context.Background(),
				"INSERT INTO timesheets (employee_id, year_month, daystotal, hourstotal, md5) VALUES ($1, $2, $3, $4, $5)",
				tms.employeeId, tms.month, tms.daysTotal, tms.hoursTotal, tms.md5)
			if err != nil {
				fmt.Print("1:")
				fmt.Println(err)
			}
			for _, day := range tms.timesheetDays {
				_, err := conn.Exec(context.Background(),
					"INSERT INTO timesheetdays (timesheet_id, type, duration, date) VALUES ($1, $2, $3, $4)",
					tms.id, day.typeOfDay, day.workDuration, day.date)
				if err != nil {
					fmt.Print("2:")
					fmt.Println(err)
				}
			}

			continue
		}
		if tms.md5 != md5 || tms.employeeId != employeeId || tms.hoursTotal != hourstotal || tms.daysTotal != daystotal || tms.month != yearMonth {
			_, err := conn.Exec(context.Background(),
				"UPDATE timesheets SET employee_id = $2, year_month = $3, daystotal = $4, hourstotal = $5, md5 = $6 WHERE timesheet_id = $1",
				tms.id, tms.employeeId, tms.month, tms.daysTotal, tms.hoursTotal, tms.md5)
			if err != nil {
				fmt.Print("3:")
				fmt.Println(err)
			}
			if tms.md5 != md5 {
				_, err := conn.Exec(context.Background(),
					"DELETE FROM timesheetsdays WHERE timesheet_id = $1", tms.id)
				if err != nil {
					fmt.Print("4:")
					fmt.Println(err)
				}
				for _, day := range tms.timesheetDays {
					_, err := conn.Exec(context.Background(),
						"INSERT INTO timesheetdays (timesheet_id, type, duration, date) VALUES ($1, $2, $3, $4)",
						tms.id, day.typeOfDay, day.workDuration, day.date)
					if err != nil {
						fmt.Print("5:")
						log.Println(err)
					}
				}
			}
		}
	}
}
