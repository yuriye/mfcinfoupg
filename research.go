package mfcinfoupg

import "fmt"

func PrintStaffByDivision(csvDirectory string) {

	divs, err := GetDivisions(csvDirectory + "sprapodr.tps.SPDR.csv")
	if err != nil {
		fmt.Print(1)
		fmt.Println(err)
		return
	}

	staff, err := GetStaff(csvDirectory + "list.tps.csv")
	if err != nil {
		fmt.Print(2)
		fmt.Println(err)
		return
	}

	divsMap := make(map[int]string)
	for _, div := range divs {
		if !div.arc {
			divsMap[div.id] = div.name
		}
	}
	staffCounts := make(map[int]int)
	for _, empl := range staff {
		if empl.arc {
			continue
		}
		staffCounts[empl.divisionId]++
	}
	for key, count := range staffCounts {
		fmt.Printf("%s - %d", divsMap[key], count)
		fmt.Println()
	}
}

func PrintActualPositions(csvDirectory string) {
	arr, _ := GetPositions(csvDirectory + "spra.tps.SDLG.csv")
	postionsMap := make(map[int]string)
	for _, pos := range arr {
		postionsMap[pos.id] = pos.name
	}
	staffArr, _ := GetStaff(csvDirectory + "list.tps.csv")
	posCounters := make(map[string]int)
	for _, empl := range staffArr {
		if empl.arc {
			continue
		}
		posCounters[postionsMap[empl.positionId]]++
	}
	for key, cntr := range posCounters {
		fmt.Printf("%s - %d", key, cntr)
		fmt.Println()
	}
}

func PrintActualPositionIds(csvDirectory string) {
	arr, _ := GetStaff(csvDirectory + "list.tps.csv")
	posIdsMap := make(map[int]int)
	for _, empl := range arr {
		posIdsMap[empl.positionId]++
	}
}

func PrintStaff(csvDirectory string) {
	arr, _ := GetStaff(csvDirectory + "list.tps.csv")
	positions, _ := GetPositions(csvDirectory + "spra.tps.SDLG.csv")
	mapOfPositions := make(map[int]string)
	for _, pos := range positions {
		mapOfPositions[pos.id] = pos.name
	}
	names := make(map[string]string)
	for _, empl := range arr {
		names[mapOfPositions[empl.positionId]] = ""
	}

	for name := range names {
		println(name)
	}
}
