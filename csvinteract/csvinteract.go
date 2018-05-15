package csvinteract

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/thomas-bamilo/financebooking/row/beneficiarycoderow"
	"github.com/thomas-bamilo/financebooking/row/ledgermaprow"
	"github.com/thomas-bamilo/financebooking/row/retailshortcoderow"
)

func ReadBeneficiaryCodeCSV(beneficiaryCodeTableP []*beneficiarycoderow.BeneficiaryCodeRow) (beneficiaryCodeTable []beneficiarycoderow.BeneficiaryCodeRow) {

	beneficiaryCodeFile, err := os.OpenFile("benef_code_map.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_open_benef_code_map.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}
	defer beneficiaryCodeFile.Close()

	if err := gocsv.UnmarshalFile(beneficiaryCodeFile, &beneficiaryCodeTableP); err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_read_benef_code_map.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}

	fmt.Printf(strconv.Itoa(len(beneficiaryCodeTableP)))

	for _, beneficiaryCodeRow := range beneficiaryCodeTableP {
		beneficiaryCodeTable = append(beneficiaryCodeTable,
			beneficiarycoderow.BeneficiaryCodeRow{
				ShortCode:       beneficiaryCodeRow.ShortCode,
				BeneficiaryCode: beneficiaryCodeRow.BeneficiaryCode,
			})
	}
	return beneficiaryCodeTable
}

func ReadRetailShortCodeRow(retailShortCodeTableP []*retailshortcoderow.RetailShortCodeRow) (retailShortCodeTable []retailshortcoderow.RetailShortCodeRow) {

	retailShortCodeFile, err := os.OpenFile("retail_supplier.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_open_retail_supplier.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}
	defer retailShortCodeFile.Close()

	if err := gocsv.UnmarshalFile(retailShortCodeFile, &retailShortCodeTableP); err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_read_retail_supplier.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}

	for _, retailShortCodeRow := range retailShortCodeTableP {
		retailShortCodeTable = append(retailShortCodeTable,
			retailshortcoderow.RetailShortCodeRow{
				ShortCode: retailShortCodeRow.ShortCode,
			})
	}
	return retailShortCodeTable

}

func ReadLedgerMapCSV(ledgerMapTableP []*ledgermaprow.LedgerMapRow) (ledgerMapTable []ledgermaprow.LedgerMapRow) {

	ledgerMapFile, err := os.OpenFile("ledger_map.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_open_ledger_map.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}
	defer ledgerMapFile.Close()

	if err := gocsv.UnmarshalFile(ledgerMapFile, &ledgerMapTableP); err != nil {
		fmt.Printf("FAILURE! %v\n", err)
		writeErrorToFile(err, `err_read_ledger_map.txt`)
		time.Sleep(30 * time.Second)
		log.Fatal(err)
	}

	for _, ledgerMapRow := range ledgerMapTableP {
		ledgerMapTable = append(ledgerMapTable,
			ledgermaprow.LedgerMapRow{
				TransactionType:      ledgerMapRow.TransactionType,
				ItemStatus:           ledgerMapRow.ItemStatus,
				PaymentMethod:        ledgerMapRow.PaymentMethod,
				ShipmentProviderName: ledgerMapRow.ShipmentProviderName,
				Ledger:               ledgerMapRow.Ledger,
				Subledger:            ledgerMapRow.Subledger,
			})
	}
	return ledgerMapTable

}

func writeErrorToFile(errr error, filename string) {
	file, err := os.Create(filename)
	checkError(err)
	defer file.Close()

	fmt.Fprintf(file, string(errr.Error()))
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
