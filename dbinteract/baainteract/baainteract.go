package baainteract

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/thomas-bamilo/financebooking/row/beneficiarycoderow"
	"github.com/thomas-bamilo/financebooking/row/ledgermaprow"
	"github.com/thomas-bamilo/financebooking/row/retailshortcoderow"
	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

func LoadValidBeneficiaryCodeToBaa(dbBaa *sql.DB, beneficiaryCodeTableValidRow []beneficiarycoderow.BeneficiaryCodeRow) {

	// prepare statement to insert values into beneficiary_code_map table
	insertBeneficiaryCodeTableStr := `INSERT INTO baa_application.finance.beneficiary_code_map (
		short_code
		,beneficiary_code) 
	VALUES (@p1,@p2)`
	insertBeneficiaryCodeTable, err := dbBaa.Prepare(insertBeneficiaryCodeTableStr)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_prepare_beneficiary_code_map.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)

	}

	csvErrorLogP := []*beneficiarycoderow.BeneficiaryCodeRow{}

	// write beneficiaryCodeTableValidRow into beneficiary_code_map table
	// and write csvErrorLog to csvErrorLog.csv
	for i := 0; i < len(beneficiaryCodeTableValidRow); i++ {
		_, err = insertBeneficiaryCodeTable.Exec(
			beneficiaryCodeTableValidRow[i].ShortCode,
			beneficiaryCodeTableValidRow[i].BeneficiaryCode,
		)
		if err != nil {
			fmt.Printf("WARNING! %v\n", err)

			csvErrorLogP = append(csvErrorLogP,
				&beneficiarycoderow.BeneficiaryCodeRow{
					Err:             string(err.Error()),
					ShortCode:       beneficiaryCodeTableValidRow[i].ShortCode,
					BeneficiaryCode: beneficiaryCodeTableValidRow[i].BeneficiaryCode,
				})

			// to write csvErrorLog to csv
			file, err := os.OpenFile("BeneficiaryCodeErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
			checkError(err)
			defer file.Close()
			// save csvErrorLog to csv
			err = gocsv.MarshalFile(&csvErrorLogP, file)
			//log.Fatal(err)

		}
		time.Sleep(1 * time.Millisecond)
	}

}

func LoadValidRetailShortCodeToBaa(dbBaa *sql.DB, retailShortCodeTableValidRow []retailshortcoderow.RetailShortCodeRow) {

	// prepare statement to insert values into retail_short_code table
	insertRetailShortCodeTableStr := `INSERT INTO baa_application.finance.retail_short_code (
		short_code) 
	VALUES (@p1)`
	insertRetailShortCodeTable, err := dbBaa.Prepare(insertRetailShortCodeTableStr)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_prepare_retail_short_code.txt`)
		log.Fatal(err)
		time.Sleep(30 * time.Second)
	}

	csvErrorLogP := []*retailshortcoderow.RetailShortCodeRow{}

	// write retailShortCodeTableValidRow into retail_short_code table
	// and write csvErrorLog to csvErrorLog.csv
	for i := 0; i < len(retailShortCodeTableValidRow); i++ {
		_, err = insertRetailShortCodeTable.Exec(
			retailShortCodeTableValidRow[i].ShortCode,
		)
		if err != nil {
			fmt.Printf("WARNING! %v\n", err)

			csvErrorLogP = append(csvErrorLogP,
				&retailshortcoderow.RetailShortCodeRow{
					Err:       string(err.Error()),
					ShortCode: retailShortCodeTableValidRow[i].ShortCode,
				})

			// to write csvErrorLog to csv
			file, err := os.OpenFile("RetailShortCodeErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
			checkError(err)
			defer file.Close()
			// save csvErrorLog to csv
			err = gocsv.MarshalFile(&csvErrorLogP, file)
			//log.Fatal(err)

		}
		time.Sleep(1 * time.Millisecond)
	}

}

func LoadValidLedgerMapToBaa(dbBaa *sql.DB, ledgerMapTableValidRow []ledgermaprow.LedgerMapRow) {

	// prepare statement to insert values into ledger_map table
	insertLedgerMapTableStr := `INSERT INTO baa_application.finance.ledger_map (
		transaction_type
		,item_status
		,payment_method 
		,shipment_provider_name
		,ledger
		,subledger) 
	VALUES (@p1,@p2,@p3,@p4,@p5,@p6)`
	insertLedgerMapTable, err := dbBaa.Prepare(insertLedgerMapTableStr)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_prepare_ledger_map.txt`)
		//log.Fatal(err)

	}

	csvErrorLogP := []*ledgermaprow.LedgerMapRow{}

	// write ledgerMapTableValidRow into ledger_map table
	// and write csvErrorLog to csvErrorLog.csv
	for i := 0; i < len(ledgerMapTableValidRow); i++ {
		_, err = insertLedgerMapTable.Exec(
			ledgerMapTableValidRow[i].TransactionType,
			ledgerMapTableValidRow[i].ItemStatus,
			ledgerMapTableValidRow[i].PaymentMethod,
			ledgerMapTableValidRow[i].ShipmentProviderName,
			ledgerMapTableValidRow[i].Ledger,
			ledgerMapTableValidRow[i].Subledger,
		)
		if err != nil {
			fmt.Printf("WARNING! %v\n", err)

			csvErrorLogP = append(csvErrorLogP,
				&ledgermaprow.LedgerMapRow{
					Err:                  string(err.Error()),
					TransactionType:      ledgerMapTableValidRow[i].TransactionType,
					ItemStatus:           ledgerMapTableValidRow[i].ItemStatus,
					PaymentMethod:        ledgerMapTableValidRow[i].PaymentMethod,
					ShipmentProviderName: ledgerMapTableValidRow[i].ShipmentProviderName,
					Ledger:               ledgerMapTableValidRow[i].Ledger,
					Subledger:            ledgerMapTableValidRow[i].Subledger,
				})

			// to write csvErrorLog to csv
			file, err := os.OpenFile("LedgerMapErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
			checkError(err)
			defer file.Close()
			// save csvErrorLog to csv
			err = gocsv.MarshalFile(&csvErrorLogP, file)
			//log.Fatal(err)

		}
		time.Sleep(1 * time.Millisecond)
	}

}

func GetValidationLedgerMapKey(dbBaa *sql.DB) []scomsrow.ScOmsRow {

	// store LedgerMapKeyQuery in a string
	ledgerMapKeyQuery := `SELECT 
	CONCAT(lm.transaction_type,'-',lm.item_status,'-',lm.payment_method,'-'lm.shipment_provider_name) 'ledger_map_key'
	FROM baa_application.finance.ledger_map lm`

	// write LedgerMapKeyQuery result to an array of scomsrow.ScOmsRow , this array of rows represents ledgerMapKeyTable
	var ledgerMapKey string
	var ledgerMapKeyTable []scomsrow.ScOmsRow

	rows, _ := dbBaa.Query(ledgerMapKeyQuery)

	for rows.Next() {
		err := rows.Scan(&ledgerMapKey)
		checkError(err)
		ledgerMapKeyTable = append(ledgerMapKeyTable,
			scomsrow.ScOmsRow{
				LedgerMapKey: ledgerMapKey,
			})
	}

	return ledgerMapKeyTable
}

func GetValidationBeneficiaryCode(dbBaa *sql.DB) []scomsrow.ScOmsRow {

	// store BeneficiaryCodeQuery in a string
	beneficiaryCodeQuery := `SELECT 
	bcm.short_code
	FROM baa_application.finance.beneficiary_code_map bcm`

	// write BeneficiaryCodeQuery result to an array of scomsrow.ScOmsRow , this array of rows represents beneficiaryCodeTable
	var shortCode string
	var beneficiaryCodeTable []scomsrow.ScOmsRow

	rows, _ := dbBaa.Query(beneficiaryCodeQuery)

	for rows.Next() {
		err := rows.Scan(&shortCode)
		checkError(err)
		beneficiaryCodeTable = append(beneficiaryCodeTable,
			scomsrow.ScOmsRow{
				ShortCode: shortCode,
			})
	}

	return beneficiaryCodeTable
}

func GetRetailShortCodeFromBaa(dbBaa *sql.DB) []scomsrow.ScOmsRow {

	// store RetailShortCodeQuery in a string
	retailShortCodeQuery := `SELECT 
	rsc.short_code
	FROM baa_application.finance.retail_short_code rsc`

	// write RetailShortCodeQuery result to an array of scomsrow.ScOmsRow , this array of rows represents retailShortCodeTable
	var shortCode string
	var retailShortCodeTable []scomsrow.ScOmsRow

	rows, _ := dbBaa.Query(retailShortCodeQuery)

	for rows.Next() {
		err := rows.Scan(&shortCode)
		checkError(err)
		retailShortCodeTable = append(retailShortCodeTable,
			scomsrow.ScOmsRow{
				ShortCode: shortCode,
			})
	}

	return retailShortCodeTable
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func writeErrorToFile(errr error, filename string) {
	file, err := os.Create(filename)
	checkError(err)
	defer file.Close()

	fmt.Fprintf(file, string(errr.Error()))
}
