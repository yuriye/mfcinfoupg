package mfcinfoupg

import (
	"crypto/md5"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	csvdirectory string

	dbcon dbcon
}

type dbcon struct {
	url      string
	user     string
	password string
}

type NamedEntity struct {
	id   int
	name string
	arc  bool
}

func (ne NamedEntity) GetId() int {
	return ne.id
}

type Position struct {
	NamedEntity
}

type Division struct {
	NamedEntity
	parentId int
}

type Employee struct {
	NamedEntity
	humanId    int
	tabNomer   int
	divisionId int
	positionId int
}

func (emp Employee) GetTabnomer() int {
	return emp.tabNomer
}

type Human struct {
	id         int
	surname    string
	firstname  string
	patronymic string
	gender     bool
	arc        bool
}

type Relocation struct {
	id               int
	employeeId       int
	relocationTypeId int
	divisionId       int
	positionId       int
	//Стандартная дата Кларион - это число дней, прошедших с 28 декабря 1800 года.
	date  time.Time
	dateE time.Time
	dateD time.Time
}

type Vacation struct {
	id         int
	employeeId int
	tabNomer   int
	//Стандартная дата Кларион - это число дней, прошедших с 28 декабря 1800 года.
	dateStart time.Time
	dateEnd   time.Time
	days      int
}

type TimesheetDay struct {
	typeOfDay    int
	workDuration float64
	date         time.Time
}

// Timesheet Табель отработанного времени
type Timesheet struct {
	id            int
	employeeId    int
	month         time.Time
	daysTotal     int
	hoursTotal    float64
	timesheetDays []TimesheetDay
	md5           string
}

func GetTimesheets(csvFName string, thisYear int) ([]Timesheet, error) {
	thisYearInt := time.Now().Year()
	thisYearStr := fmt.Sprintf("%d", thisYearInt)
	println(thisYearStr)
	arr, err := GetArray(strings.Replace(csvFName, "yyyy.", thisYearStr+".", 1))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}

	employeeIdInd := fieldNames["Tabn"]
	monthInd := fieldNames["Mm"]
	daysTotalInd := fieldNames["Day"]
	hoursTotalInd := fieldNames["Hour"]
	recs := make([]Timesheet, 0, 250)

	for _, rec := range arr[1:] {
		tmsheet := Timesheet{}
		tmsheet.employeeId, _ = strconv.Atoi(rec[employeeIdInd])
		monthNumber, _ := strconv.Atoi(rec[monthInd])
		tmsheet.month = time.Date(thisYear, time.Month(monthNumber), 1, 0, 0, 0, 0, time.UTC)
		tmsheet.hoursTotal, _ = strconv.ParseFloat(rec[hoursTotalInd], 64)
		tmsheet.daysTotal, _ = strconv.Atoi(rec[daysTotalInd])
		daysInMonth := time.Date(thisYear, time.Month(monthNumber)+1, 0, 0, 0, 0, 0, time.UTC).Day()
		tmsheet.timesheetDays = make([]TimesheetDay, daysInMonth)
		tmpstr := strconv.Itoa(tmsheet.employeeId) + ";"
		for d := 0; d < daysInMonth; d++ {
			tsDay := TimesheetDay{}
			ind := fieldNames["Tabel["+strconv.Itoa(d)+"]"]
			tsDay.typeOfDay, _ = strconv.Atoi(rec[ind])
			tmpstr += strconv.Itoa(tsDay.typeOfDay) + ";"
			ind = fieldNames["Tabelh["+strconv.Itoa(d)+"]"]
			tsDay.workDuration, _ = strconv.ParseFloat(rec[ind], 64)
			tsDay.date = time.Date(thisYear, time.Month(monthNumber), d+1, 0, 0, 0, 0, time.UTC)
			tmpstr += fmt.Sprintf("%v", tsDay.workDuration) + ";"
			tmsheet.timesheetDays = append(tmsheet.timesheetDays, tsDay)
		}
		h := md5.New()
		_, err := io.WriteString(h, tmpstr)
		if err != nil {
			return nil, err
		}
		s := h.Sum(nil)
		tmsheet.md5 = fmt.Sprintf("%x", s)
		recs = append(recs, tmsheet)
	}
	return recs, nil
}

func GetArray(csvFName string) ([][]string, error) {
	f, err := os.Open(csvFName)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {

		}
	}(f)
	r := csv.NewReader(f)
	r.Comma = ';'
	records, err := r.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	return records, nil
}

func GetPositions(csvFName string) ([]Position, error) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Id"]
	nameInd := fieldNames["Name"]
	arcInd := fieldNames["Archive"]
	recs := make([]Position, 0, 50)
	var pos Position
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
		recs = append(recs, pos)
	}
	return recs, nil
}

func GetDivisions(csvFName string) ([]Division, error) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Id"]
	nameInd := fieldNames["Name"]
	arcInd := fieldNames["Archive"]
	recs := make([]Division, 0, 50)
	var div Division
	for _, rec := range arr[1:] {
		div = Division{}
		div.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			fmt.Println(err)
			continue
		}
		div.name = strings.Trim(rec[nameInd], " ")

		if rec[arcInd] == "1" {
			div.arc = true
		}
		recs = append(recs, div)
	}
	return recs, nil
}

func GetStaff(csvFName string) ([]Employee, error) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Tabn"]
	//nameInd := fieldNames["Name"]
	arcInd := fieldNames["Archive"]
	humanIdInd := fieldNames["Fizlist"]
	tabNomerInd := fieldNames["Tabnomer"]
	divisionIdInd := fieldNames["Podr"]
	positionIdInd := fieldNames["Dolg"]

	recs := make([]Employee, 0, 50)
	var emp Employee
	for _, rec := range arr[1:] {
		emp = Employee{}
		emp.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			log.Println(err)
			continue
		}
		//emp.name = strings.Trim(rec[nameInd], " ")

		if rec[arcInd] == "1" {
			emp.arc = true
		}
		emp.humanId, _ = strconv.Atoi(rec[humanIdInd])
		emp.tabNomer, _ = strconv.Atoi(rec[tabNomerInd])
		emp.positionId, _ = strconv.Atoi(rec[positionIdInd])
		emp.divisionId, _ = strconv.Atoi(rec[divisionIdInd])
		recs = append(recs, emp)
	}
	return recs, nil
}

func GetVacations(csvFName string) ([]Vacation, error) {
	arr, err := GetArray(csvFName)

	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}

	idInd := fieldNames["RecNo"]
	tabNomerInd := fieldNames["Tabn"]
	dateStartInd := fieldNames["Dataplb"]
	dateEndInd := fieldNames["Dataple"]
	daysInd := fieldNames["Day"]
	recs := make([]Vacation, 0, 50)
	var vacation Vacation
	for _, rec := range arr[1:] {
		vacation = Vacation{}
		vacation.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			log.Println(err)
			continue
		}
		vacation.tabNomer, _ = strconv.Atoi(rec[tabNomerInd])
		tmp, err := strconv.Atoi(rec[dateStartInd])
		if err == nil {
			vacation.dateStart = ClarT2UnixT(int64(tmp))
		}
		tmp, err = strconv.Atoi(rec[dateEndInd])
		if err == nil {
			vacation.dateStart = ClarT2UnixT(int64(tmp))
		}
		vacation.days, _ = strconv.Atoi(rec[daysInd])

		recs = append(recs, vacation)
	}

	return recs, nil
}

func GetPeople(csvFName string) ([]Human, error) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Id"]
	surnameInd := fieldNames["Name"]
	firstnameInd := fieldNames["Fname"]
	patronymicInd := fieldNames["Lname"]
	genderInd := fieldNames["Pol"]
	arcInd := fieldNames["Archive"]
	recs := make([]Human, 0, 50)
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
		if rec[genderInd] == "1" {
			hum.gender = true
		}
		if rec[arcInd] == "1" {
			hum.arc = true
		}
		recs = append(recs, hum)
	}
	return recs, nil
}

func GetRelocations(csvFName string) ([]Relocation, error) {
	arr, err := GetArray(csvFName)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	fieldNames := make(map[string]int)
	for ind, name := range arr[0] {
		fieldNames[name] = ind
	}
	idInd := fieldNames["Id"]
	employeeIdInd := fieldNames["Tabn"]
	divisionIdInd := fieldNames["Podr"]
	positionIdInd := fieldNames["Dolg"]
	typeOfRelocInd := fieldNames["Vidper"]
	dateInd := fieldNames["Data"]
	dateEInd := fieldNames["Datae"]
	dateDInd := fieldNames["Datad"]

	recs := make([]Relocation, 0, 50)
	var rel Relocation
	for _, rec := range arr[1:] {
		rel = Relocation{}
		rel.id, err = strconv.Atoi(rec[idInd])
		if err != nil {
			fmt.Println(err)
			continue
		}

		rel.employeeId, _ = strconv.Atoi(rec[employeeIdInd])
		rel.positionId, _ = strconv.Atoi(rec[positionIdInd])
		rel.divisionId, _ = strconv.Atoi(rec[divisionIdInd])
		rel.relocationTypeId, _ = strconv.Atoi(rec[typeOfRelocInd])

		tmp, err := strconv.Atoi(rec[dateInd])
		if err == nil {
			rel.date = ClarT2UnixT(int64(tmp))
		}

		tmp, err = strconv.Atoi(rec[dateEInd])
		if err == nil {
			rel.dateE = ClarT2UnixT(int64(tmp))
		}

		tmp, err = strconv.Atoi(rec[dateDInd])
		if err == nil {
			rel.dateD = ClarT2UnixT(int64(tmp))
		}

		recs = append(recs, rel)
	}
	return recs, nil
}

func GetNamesMap(ents []NamedEntity) map[int]string {
	namedMap := make(map[int]string)
	for _, ent := range ents {
		namedMap[ent.id] = ent.name
	}
	return namedMap
}
