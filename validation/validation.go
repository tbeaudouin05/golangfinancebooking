package validation

import (
	"errors"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/thomas-bamilo/email/goemail"
	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

// LedgerMapKey -------------------------------------------------------------------------

func MissingLedgerMapKey(ledgerMapKeyTable, scOmsTable []scomsrow.ScOmsRow) (missingLedgerMapKeyTable []scomsrow.ScOmsRow) {

	// initialize ledgerMapKeyMap with ledgerMapKeyTable
	ledgerMapKeyMap := make(map[string]bool)
	for _, ledgerMapKeyRow := range ledgerMapKeyTable {
		ledgerMapKeyMap[ledgerMapKeyRow.LedgerMapKey] = true
	}

	// for each scOmsRow in scOmsTable
	// if scOmsRow.LedgerMapKey is in ledgerMapKeyMap
	// then do nothing
	// else add scOmsRow to missingLedgerMapKeyTable
	// AND add scOmsRow.LedgerMapKey to ledgerMapKeyMap (not to return several times the same ledgerMapKey)
	for _, scOmsRow := range scOmsTable {
		if _, ok := ledgerMapKeyMap[scOmsRow.LedgerMapKey]; !ok {
			missingLedgerMapKeyTable = append(missingLedgerMapKeyTable, scOmsRow)
			ledgerMapKeyMap[scOmsRow.LedgerMapKey] = true
		}

	}
	return missingLedgerMapKeyTable

}

// IfMissingLedgerMap STOPs the booking process if any missing ledger_map (we can make it less blocking in the future)
func IfMissingLedgerMap(missingLedgerMapKeyTable []scomsrow.ScOmsRow) {

	if len(missingLedgerMapKeyTable) > 0 {
		var csvErrorLogP []*scomsrow.ScOmsRow
		for i := 0; i < len(missingLedgerMapKeyTable); i++ {
			csvErrorLogP = append(csvErrorLogP,
				&scomsrow.ScOmsRow{
					TransactionType:      missingLedgerMapKeyTable[i].TransactionType,
					ItemStatus:           missingLedgerMapKeyTable[i].ItemStatus,
					PaymentMethod:        missingLedgerMapKeyTable[i].PaymentMethod,
					ShipmentProviderName: missingLedgerMapKeyTable[i].ShipmentProviderName,
					Ledger:               0,
					Subledger:            0,
				})
		}
		// to write csvErrorLog to csv
		file, err := os.OpenFile("FinanceBookingErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
		checkError(err)
		defer file.Close()
		// save csvErrorLog to csv
		err = gocsv.MarshalFile(&csvErrorLogP, file)
		checkError(err)
		time.Sleep(5 * time.Second)
		// send an email with FinanceBookingErrorLog.csv in attachment
		goemail.GoEmail()
		// creating an error and calling checkError() effectively stops the booking process
		err = errors.New("FAILURE: missing ledger_map, please see FinanceBookingErrorLog.csv for more details")
		checkError(err)
	}
}

// BeneficiaryCode ---------------------------------------------------------------------------------------------------------------------------------------------------

// MissingBeneficiaryCode filters out ShortCode found in beneficiary_code_map table of BAA database from sellerCenterTable and outputs missingBeneficiaryCodeTable.
// missingBeneficiaryCodeTable should be empty - otherwise, beneficiary_code_map needs to be updated
func MissingBeneficiaryCode(beneficiaryCodeTable, sellerCenterTable []scomsrow.ScOmsRow) (missingBeneficiaryCodeTable []scomsrow.ScOmsRow) {

	// initialize beneficiaryCodeMap with beneficiaryCodeTable
	beneficiaryCodeMap := make(map[string]bool)
	for _, beneficiaryCodeRow := range beneficiaryCodeTable {
		beneficiaryCodeMap[beneficiaryCodeRow.ShortCode] = true
	}

	// check if any BeneficiaryCode from sellerCenterTable is not in beneficiaryCodeMap - if not, add the BeneficiaryCode to missingBeneficiaryCodeTable
	for _, beneficiaryCodeRow := range sellerCenterTable {
		if _, ok := beneficiaryCodeMap[beneficiaryCodeRow.ShortCode]; !ok {
			missingBeneficiaryCodeTable = append(missingBeneficiaryCodeTable, beneficiaryCodeRow)
		}

	}
	return missingBeneficiaryCodeTable

}

// IfMissingBeneficiaryCode STOPs the booking process if any missing short_code (we can make it less blocking in the future)
func IfMissingBeneficiaryCode(missingBeneficiaryCodeTable []scomsrow.ScOmsRow) {
	if len(missingBeneficiaryCodeTable) > 0 {
		var csvErrorLogP []*scomsrow.ScOmsRow
		for i := 0; i < len(missingBeneficiaryCodeTable); i++ {
			csvErrorLogP = append(csvErrorLogP,
				&scomsrow.ScOmsRow{
					ShortCode: missingBeneficiaryCodeTable[i].ShortCode,
				})

		}
		// to write csvErrorLog to csv
		file, err := os.OpenFile("FinanceBookingErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
		checkError(err)
		defer file.Close()
		// save csvErrorLog to csv
		err = gocsv.MarshalFile(&csvErrorLogP, file)
		checkError(err)
		time.Sleep(5 * time.Second)
		// send an email with FinanceBookingErrorLog.csv in attachment
		goemail.GoEmail()
		// creating an error and calling checkError() effectively stops the booking process
		err = errors.New("FAILURE: missing beneficiary_code, please see FinanceBookingErrorLog.csv for more details")
		checkError(err)
	}
}

// FilterRetailShortCode filters out ShortCode found in retail_short_code table of BAA database from sellerCenterTable and outputs sellerCenterTableNoRetail: a table without RetailShortCode
func FilterRetailShortCode(retailShortCodeTable, sellerCenterTable []scomsrow.ScOmsRow) (sellerCenterTableNoRetail []scomsrow.ScOmsRow) {

	// initialize RetailShortCodeMap with retailShortCodeTable
	RetailShortCodeMap := make(map[string]bool)
	for _, RetailShortCodeRow := range retailShortCodeTable {
		RetailShortCodeMap[RetailShortCodeRow.ShortCode] = true
	}

	// for each sellerCenterRow in sellerCenterTable
	// if sellerCenterRow.ShortCode is in RetailShortCodeMap
	// then do nothing
	// else append sellerCenterRow to sellerCenterTableNoRetail
	for _, sellerCenterRow := range sellerCenterTable {
		if _, ok := RetailShortCodeMap[sellerCenterRow.ShortCode]; !ok {
			sellerCenterTableNoRetail = append(sellerCenterTableNoRetail, sellerCenterRow)
		}

	}
	return sellerCenterTableNoRetail

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
