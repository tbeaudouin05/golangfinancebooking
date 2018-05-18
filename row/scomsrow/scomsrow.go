package scomsrow

import (
	"log"
	"os"
	"time"

	"github.com/thomas-bamilo/email/goemail"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"
	"github.com/gocarina/gocsv"
)

// ScOmsRow represents a row of data coming from Seller Center and OMS used in Finance Booking process
type ScOmsRow struct {
	Err string `json:"error"`
	// Seller Center
	IDTransaction          int     `json:"id_transaction"`
	OmsIDSalesOrderItem    int     `json:"oms_id_sales_order_item"`
	OrderNr                string  `json:"order_nr"`
	IDSupplier             int     `json:"id_supplier"`
	ShortCode              string  `json:"short_code"`
	SupplierName           string  `json:"supplier_name"`
	IDTransactionType      int     `json:"id_transaction_type"`
	TransactionType        string  `json:"transaction_type"`
	TransactionValue       float32 `json:"transaction_value"`
	IDTransactionStatement int     `json:"id_transaction_statement"`
	StatementStartDate     string  `json:"statement_start_date"`
	StatementEndDate       string  `json:"statement_end_date"`
	Comment                string  `json:"comment"`
	// OMS
	ItemStatus           string  `json:"item_status"`
	PaymentMethod        string  `json:"payment_method"`
	ShipmentProviderName string  `json:"shipment_provider_name"`
	PaidPrice            float32 `json:"paid_price"`
	// Finance
	LedgerMapKey    string `json:"ledger_map_key"`
	Ledger          int    `json:"ledger"`
	Subledger       int    `json:"subledger"`
	BeneficiaryCode int    `json:"beneficiary_code"`
}

// Seller Center row ------------------------------------------------------------------------------------
// define validation for each field of SellerCenterRow
func (row ScOmsRow) validateScRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.IDTransaction, validation.Required),
		validation.Field(&row.OmsIDSalesOrderItem, validation.Required),
		validation.Field(&row.OrderNr, validation.Required, is.Int),
		validation.Field(&row.IDSupplier, validation.Required),
		validation.Field(&row.ShortCode, validation.Required),
		validation.Field(&row.IDTransactionType, validation.Required),
		validation.Field(&row.TransactionType, validation.Required),
		validation.Field(&row.TransactionValue, validation.Required),
		validation.Field(&row.IDTransactionStatement, validation.Required),
		validation.Field(&row.StatementStartDate, validation.Required),
		validation.Field(&row.StatementEndDate, validation.Required),
	)
}

// FilterSellerCenterTable splits SellerCenterTable into SellerCenterTableValidRow and SellerCenterTableInvalidRow
func FilterSellerCenterTable(sellerCenter []ScOmsRow) (SellerCenterTableValidRow, SellerCenterTableInvalidRow []ScOmsRow) {

	SellerCenterTableValidRow = filterSc(sellerCenter, isValidScRowFormat)
	SellerCenterTableInvalidRow = filterSc(sellerCenter, isInvalidScRowFormat)

	// add error message to SellerCenterTableInvalidRow
	for i := 0; i < len(SellerCenterTableInvalidRow); i++ {
		SellerCenterTableInvalidRow[i].Err = SellerCenterTableInvalidRow[i].validateScRowFormat().Error()
	}

	return SellerCenterTableValidRow, SellerCenterTableInvalidRow

}

// IfInvalidSellerCenterRow outputs an error csv and sends it to Finance
// if there is any row in sellerCenterInvalidTable
func IfInvalidSellerCenterRow(sellerCenterInvalidTable []ScOmsRow) {
	if len(sellerCenterInvalidTable) > 0 {
		var csvErrorLogP []*ScOmsRow
		for i := 0; i < len(sellerCenterInvalidTable); i++ {
			csvErrorLogP = append(csvErrorLogP,
				&ScOmsRow{
					Err:                 sellerCenterInvalidTable[i].Err,
					OmsIDSalesOrderItem: sellerCenterInvalidTable[i].OmsIDSalesOrderItem,
					OrderNr:             sellerCenterInvalidTable[i].OrderNr,
					ShortCode:           sellerCenterInvalidTable[i].ShortCode,
					SupplierName:        sellerCenterInvalidTable[i].SupplierName,
					TransactionType:     sellerCenterInvalidTable[i].TransactionType,
					TransactionValue:    sellerCenterInvalidTable[i].TransactionValue,
					StatementStartDate:  sellerCenterInvalidTable[i].StatementStartDate,
					StatementEndDate:    sellerCenterInvalidTable[i].StatementEndDate,
				})
		}
		// to write csvErrorLog to csv
		file, err := os.OpenFile("FinanceBookingErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
		checkError(err)
		defer file.Close()
		// save csvErrorLog to csv
		err = gocsv.MarshalFile(&csvErrorLogP, file)
		log.Println("WARNING: sellerCentertable had some invalid rows, please see FinanceBookingErrorLog.csv")
		time.Sleep(3 * time.Second)
		// send csv by email
		goemail.GoEmail()

	}
}

// filter an array of SellerCenterRow with pointer
func filterSc(unfilteredTable []ScOmsRow, test func(*ScOmsRow) bool) (filteredTable []ScOmsRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if SellerCenterRow has valid format
func isValidScRowFormat(row *ScOmsRow) bool {

	err := row.validateScRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerCenterRow has invalid format
func isInvalidScRowFormat(row *ScOmsRow) bool {

	err := row.validateScRowFormat()
	if err != nil {
		return true
	}
	return false

}

// ScOmsRow ------------------------------------------------------------------------------------------------
// define validation for each field of ScOmsRow
func (row ScOmsRow) validateScOmsRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.IDTransaction, validation.Required),
		validation.Field(&row.OmsIDSalesOrderItem, validation.Required),
		validation.Field(&row.OrderNr, validation.Required, is.Int),
		validation.Field(&row.IDSupplier, validation.Required),
		validation.Field(&row.ShortCode, validation.Required),
		validation.Field(&row.IDTransactionType, validation.Required),
		validation.Field(&row.TransactionType, validation.Required),
		validation.Field(&row.TransactionValue, validation.Required),
		validation.Field(&row.IDTransactionStatement, validation.Required),
		validation.Field(&row.StatementStartDate, validation.Required),
		validation.Field(&row.StatementEndDate, validation.Required),
		validation.Field(&row.ItemStatus, validation.Required),
		validation.Field(&row.PaymentMethod, validation.Required),
		validation.Field(&row.ShipmentProviderName, validation.Required),
		validation.Field(&row.PaidPrice, validation.Required),
	)
}

// FilterScOmsTable splits ScOmsTable into ScOmsTableValidRow and ScOmsTableInvalidRow
func FilterScOmsTable(sellerCenter []ScOmsRow) (ScOmsTableValidRow, ScOmsTableInvalidRow []ScOmsRow) {

	ScOmsTableValidRow = filterScOms(sellerCenter, isValidScOmsRowFormat)
	ScOmsTableInvalidRow = filterScOms(sellerCenter, isInvalidScOmsRowFormat)

	// add error message to ScOmsTableInvalidRow
	for i := 0; i < len(ScOmsTableInvalidRow); i++ {
		ScOmsTableInvalidRow[i].Err = ScOmsTableInvalidRow[i].validateScOmsRowFormat().Error()
	}

	return ScOmsTableValidRow, ScOmsTableInvalidRow

}

// IfInvalidScOmsRow outputs an error csv and sends it to Finance
// if there is any row in scOmsInvalidTable
func IfInvalidScOmsRow(scOmsInvalidTable []ScOmsRow) {
	if len(scOmsInvalidTable) > 0 {
		var csvErrorLogP []*ScOmsRow
		for i := 0; i < len(scOmsInvalidTable); i++ {
			csvErrorLogP = append(csvErrorLogP,
				&ScOmsRow{
					Err:                  scOmsInvalidTable[i].Err,
					OmsIDSalesOrderItem:  scOmsInvalidTable[i].OmsIDSalesOrderItem,
					OrderNr:              scOmsInvalidTable[i].OrderNr,
					ShortCode:            scOmsInvalidTable[i].ShortCode,
					SupplierName:         scOmsInvalidTable[i].SupplierName,
					TransactionType:      scOmsInvalidTable[i].TransactionType,
					TransactionValue:     scOmsInvalidTable[i].TransactionValue,
					ItemStatus:           scOmsInvalidTable[i].ItemStatus,
					PaymentMethod:        scOmsInvalidTable[i].PaymentMethod,
					ShipmentProviderName: scOmsInvalidTable[i].ShipmentProviderName,
					PaidPrice:            scOmsInvalidTable[i].PaidPrice,
				})
		}
		// to write csvErrorLog to csv
		file, err := os.OpenFile("FinanceBookingErrorLog.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
		checkError(err)
		defer file.Close()
		// save csvErrorLog to csv
		err = gocsv.MarshalFile(&csvErrorLogP, file)
		log.Println("WARNING: scOmstable had some invalid rows, please see FinanceBookingErrorLog.csv")
		time.Sleep(3 * time.Second)
		// send csv by email
		goemail.GoEmail()

	}
}

// filter an array of ScOmsRow
func filterScOms(unfilteredTable []ScOmsRow, test func(*ScOmsRow) bool) (filteredTable []ScOmsRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if ScOmsRow has valid format
func isValidScOmsRowFormat(row *ScOmsRow) bool {

	err := row.validateScOmsRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if ScOmsRow has invalid format
func isInvalidScOmsRowFormat(row *ScOmsRow) bool {

	err := row.validateScOmsRowFormat()
	if err != nil {
		return true
	}
	return false

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
