package retailshortcoderow

import (
	"regexp"

	validation "github.com/go-ozzo/ozzo-validation"
)

// RetailShortCodeRow represents a row of the table LedgerMapTable
type RetailShortCodeRow struct {
	Err       string `csv:"error"`
	ShortCode string `csv:"short_code"`
}

// define validation for each field of RetailShortCodeRow
func (row RetailShortCodeRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.ShortCode, validation.Required, validation.Match(regexp.MustCompile("^IR[[:digit:]]{2}[0-9A-Z]{3}$"))),
	)
}

// FilterRetailShortCodeTable splits RetailShortCodeTable into RetailShortCodeTableValidRow and RetailShortCodeTableInvalidRow
func FilterRetailShortCodeTable(retailShortCodeable []RetailShortCodeRow) (RetailShortCodeTableValidRow, RetailShortCodeTableInvalidRow []RetailShortCodeRow) {

	RetailShortCodeTableValidRow = filterPointer(retailShortCodeable, isValidRowFormat)
	RetailShortCodeTableInvalidRow = filterPointer(retailShortCodeable, isInvalidRowFormat)

	// add error message to RetailShortCodeTableInvalidRow
	for i := 0; i < len(RetailShortCodeTableInvalidRow); i++ {

		RetailShortCodeTableInvalidRow[i].Err = RetailShortCodeTableInvalidRow[i].validateRowFormat().Error()
	}

	return RetailShortCodeTableValidRow, RetailShortCodeTableInvalidRow

}

// filter an array of RetailShortCodeRow with pointer
func filterPointer(unfilteredTable []RetailShortCodeRow, test func(*RetailShortCodeRow) bool) (filteredTable []RetailShortCodeRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if RetailShortCodeRow has valid format
func isValidRowFormat(row *RetailShortCodeRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerRejectionRow has invalid format
func isInvalidRowFormat(row *RetailShortCodeRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}
