package ledgermaprow

import (
	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"
)

// LedgerMapRow represents a row of the table LedgerMapTable
type LedgerMapRow struct {
	Err                  string `csv:"error"`
	TransactionType      string `csv:"transaction_type"`
	ItemStatus           string `csv:"item_status"`
	PaymentMethod        string `csv:"payment_method"`
	ShipmentProviderName string `csv:"shipment_provider_name"`
	Ledger               string `csv:"ledger"`
	Subledger            string `csv:"subledger"`
}

// define validation for each field of LedgerMapRow
func (row LedgerMapRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.TransactionType, validation.Required, validation.In(`Item Price`, `Item Price Credit`)),
		validation.Field(&row.ItemStatus, validation.Required, validation.In(
			`delivered`,
			`closed`,
			`refund_reject`,
			`replaced`,
			`refund_completed`,
			`canceled`,
			`being_returned`,
			`ready_for_pickup`,
			`shipped`,
			`pickup_return_pending`,
			`packed`,
			`returned`,
			`returned_qcfailed`,
			`refund_pending`,
			`ready_for_refund`,
			`replacement_pending`,
		)),
		validation.Field(&row.PaymentMethod, validation.Required, validation.In(
			`CashOnDelivery`,
			`SEP`,
			`BankDeposit`,
			`PEC`,
			`Jiring`,
			`AsanPardakht`,
			`Irankish`,
		)),
		validation.Field(&row.ShipmentProviderName, validation.Required, validation.In(
			`Tipax`,
			`Bamilo Transportation System`,
			`Post`,
			`Sent by Seller`,
			`Chaapaar`,
			`skynet_intl`,
			`NA`,
			`Pishtaz`,
			`PostExpress`,
			`Bamilo Dropshipping`,
			`TPG`,
			`Tehran Orders`,
			`Tpx`,
		)),
		validation.Field(&row.Ledger, validation.Required, is.Int, validation.In(
			`13004`,
			`94001`,
			`33001`,
			`33002`,
			`84006`,
		)),
		validation.Field(&row.Subledger, is.Int, validation.In(
			`33002`,
			`4000000002`,
			`4000000061`,
			`4000000062`,
			`4000000196`,
			`4000000209`,
			`4000000003`,
			`4000000001`,
			`4000000006`,
		)),
	)
}

// FilterLedgerMapTable splits LedgerMapTable into LedgerMapTableValidRow and LedgerMapTableInvalidRow
func FilterLedgerMapTable(ledgerMapTable []LedgerMapRow) (LedgerMapTableValidRow, LedgerMapTableInvalidRow []LedgerMapRow) {

	LedgerMapTableValidRow = filterPointer(ledgerMapTable, isValidRowFormat)
	LedgerMapTableInvalidRow = filterPointer(ledgerMapTable, isInvalidRowFormat)

	// add error message to LedgerMapTableInvalidRow
	for i := 0; i < len(LedgerMapTableInvalidRow); i++ {
		LedgerMapTableInvalidRow[i].Err = LedgerMapTableInvalidRow[i].validateRowFormat().Error()
	}

	return LedgerMapTableValidRow, LedgerMapTableInvalidRow

}

// filter an array of LedgerMapRow with pointer
func filterPointer(unfilteredTable []LedgerMapRow, test func(*LedgerMapRow) bool) (filteredTable []LedgerMapRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if LedgerMapRow has valid format
func isValidRowFormat(row *LedgerMapRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerRejectionRow has invalid format
func isInvalidRowFormat(row *LedgerMapRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}
