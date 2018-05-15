package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/thomas-bamilo/financebooking/csvinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/baainteract"
	"github.com/thomas-bamilo/financebooking/row/beneficiarycoderow"
	"github.com/thomas-bamilo/financebooking/row/ledgermaprow"
	"github.com/thomas-bamilo/financebooking/row/retailshortcoderow"
	"github.com/thomas-bamilo/sql/connectdb"

	survey "gopkg.in/AlecAivazis/survey.v1"
)

var qs = []*survey.Question{
	{
		Name:     "Username",
		Prompt:   &survey.Input{Message: "What is your username?"},
		Validate: survey.Required,
	},
	{
		Name:     "Password",
		Prompt:   &survey.Password{Message: "What is your password?"},
		Validate: survey.Required,
	},
	{
		Name: "File",
		Prompt: &survey.Select{
			Message: "Choose the file to upload:",
			Help:    "The file should be a CSV with the exact same name in the same folder as the .exe file",
			Options: []string{"benef_code_map.csv", "retail_supplier.csv", "ledger_map.csv"},
			Default: "benef_code_map.csv",
		},
	},
}

func main() {

	/*f, err := os.OpenFile("logfile"+time.Now().Format("20060102150405")+".txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)*/

	answers := struct {
		Username string
		Password string
		File     string
	}{}

	// perform the questions
	err := survey.Ask(qs, &answers)
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if answers.Username == `shirin` && answers.Password == `gofinance` {
		fmt.Println("The file should be a CSV in the same folder as the .exe file with the exact name:", answers.File)
		fmt.Println("Please wait...")
		switch answers.File {
		case "benef_code_map.csv":
			beneficiaryCodeTableP := []*beneficiarycoderow.BeneficiaryCodeRow{}
			beneficiaryCodeTable := csvinteract.ReadBeneficiaryCodeCSV(beneficiaryCodeTableP)
			beneficiaryCodeTableValidRow, beneficiaryCodeTableInvalidRow := beneficiarycoderow.FilterBeneficiaryCodeTable(beneficiaryCodeTable)
			if len(beneficiaryCodeTableInvalidRow) > 0 {
				var csvErrorLogP []*beneficiarycoderow.BeneficiaryCodeRow
				for i := 0; i < len(beneficiaryCodeTableInvalidRow); i++ {
					csvErrorLogP = append(csvErrorLogP,
						&beneficiarycoderow.BeneficiaryCodeRow{
							Err:             beneficiaryCodeTableInvalidRow[i].Err,
							ShortCode:       beneficiaryCodeTableInvalidRow[i].ShortCode,
							BeneficiaryCode: beneficiaryCodeTableInvalidRow[i].BeneficiaryCode,
						})
				}
				// to write csvErrorLog to csv
				file, err := os.OpenFile("BeneficiaryCodeErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
				checkError(err)
				defer file.Close()
				// save csvErrorLog to csv
				err = gocsv.MarshalFile(&csvErrorLogP, file)
				fmt.Println("FAILURE: format of beneficiaryCode is wrong, please see BeneficiaryCodeErrorLog.csv for more details")
				time.Sleep(30 * time.Second)

			} else {
				dbBaa := connectdb.ConnectToBaa()
				defer dbBaa.Close()
				baainteract.LoadValidBeneficiaryCodeToBaa(dbBaa, beneficiaryCodeTableValidRow)
				fmt.Println("SUCCESS: upload of benef_code_map.csv successful!")
				time.Sleep(30 * time.Second)

			}

		case "retail_supplier.csv":
			retailShortCodeTableP := []*retailshortcoderow.RetailShortCodeRow{}
			retailShortCodeTable := csvinteract.ReadRetailShortCodeRow(retailShortCodeTableP)
			retailShortCodeTableValidRow, retailShortCodeTableInvalidRow := retailshortcoderow.FilterRetailShortCodeTable(retailShortCodeTable)

			if len(retailShortCodeTableInvalidRow) > 0 {
				var csvErrorLogP []*retailshortcoderow.RetailShortCodeRow
				for i := 0; i < len(retailShortCodeTableInvalidRow); i++ {
					csvErrorLogP = append(csvErrorLogP,
						&retailshortcoderow.RetailShortCodeRow{
							Err:       retailShortCodeTableInvalidRow[i].Err,
							ShortCode: retailShortCodeTableInvalidRow[i].ShortCode,
						})
				}
				// to write csvErrorLog to csv
				file, err := os.OpenFile("RetailShortCodeErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
				checkError(err)
				defer file.Close()
				// save csvErrorLog to csv
				err = gocsv.MarshalFile(&csvErrorLogP, file)
				fmt.Println("FAILURE: format of retailShortCode is wrong, please see RetailShortCodeErrorLog.csv for more details")
				time.Sleep(30 * time.Second)

			} else {
				dbBaa := connectdb.ConnectToBaa()
				defer dbBaa.Close()
				baainteract.LoadValidRetailShortCodeToBaa(dbBaa, retailShortCodeTableValidRow)
				fmt.Println("SUCCESS: upload of retail_supplier.csv successful!")
				time.Sleep(30 * time.Second)
			}
		case "ledger_map.csv":
			var ledgerMapTableP []*ledgermaprow.LedgerMapRow
			ledgerMapTable := csvinteract.ReadLedgerMapCSV(ledgerMapTableP)
			ledgerMapTableValidRow, ledgerMapTableInvalidRow := ledgermaprow.FilterLedgerMapTable(ledgerMapTable)

			if len(ledgerMapTableInvalidRow) > 0 {
				var csvErrorLogP []*ledgermaprow.LedgerMapRow
				for i := 0; i < len(ledgerMapTableInvalidRow); i++ {
					csvErrorLogP = append(csvErrorLogP,
						&ledgermaprow.LedgerMapRow{
							Err:                  ledgerMapTableInvalidRow[i].Err,
							TransactionType:      ledgerMapTableInvalidRow[i].TransactionType,
							ItemStatus:           ledgerMapTableInvalidRow[i].ItemStatus,
							PaymentMethod:        ledgerMapTableInvalidRow[i].PaymentMethod,
							ShipmentProviderName: ledgerMapTableInvalidRow[i].ShipmentProviderName,
							Ledger:               ledgerMapTableInvalidRow[i].Ledger,
							Subledger:            ledgerMapTableInvalidRow[i].Subledger,
						})
				}
				// to write csvErrorLog to csv
				file, err := os.OpenFile("LedgerMapErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
				checkError(err)
				defer file.Close()
				// save csvErrorLog to csv
				err = gocsv.MarshalFile(&csvErrorLogP, file)
				fmt.Println("FAILURE: format of ledgerMap is wrong, please see LedgerMapErrorLog.csv for more details")
				time.Sleep(30 * time.Second)

			} else {
				dbBaa := connectdb.ConnectToBaa()
				defer dbBaa.Close()
				baainteract.LoadValidLedgerMapToBaa(dbBaa, ledgerMapTableValidRow)
				fmt.Println("SUCCESS: upload of ledger_map.csv successful!")
				time.Sleep(30 * time.Second)
			}
		}

	} else {
		fmt.Println("Wrong password! The Police is on its way!")
		time.Sleep(30 * time.Second)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
