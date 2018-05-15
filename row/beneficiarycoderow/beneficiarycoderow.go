package beneficiarycoderow

import (
	"regexp"

	validation "github.com/go-ozzo/ozzo-validation"
	"github.com/go-ozzo/ozzo-validation/is"
)

// BeneficiaryCodeRow represents a row of the table LedgerMapTable
type BeneficiaryCodeRow struct {
	Err             string `csv:"error"`
	ShortCode       string `csv:"short_code"`
	BeneficiaryCode string `csv:"beneficiary_code"`
}

// define validation for each field of BeneficiaryCodeRow
func (row BeneficiaryCodeRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		validation.Field(&row.ShortCode, validation.Required, validation.Match(regexp.MustCompile("^IR[[:digit:]]{2}[0-9A-Z]{3}$"))),
		validation.Field(&row.BeneficiaryCode, validation.Required, is.Int, validation.Match(regexp.MustCompile("^3[[:digit:]]{9}"))),
	)
}

// FilterBeneficiaryCodeTable splits BeneficiaryCodeTable into BeneficiaryCodeTableValidRow and BeneficiaryCodeTableInvalidRow
func FilterBeneficiaryCodeTable(beneficiaryCode []BeneficiaryCodeRow) (BeneficiaryCodeTableValidRow, BeneficiaryCodeTableInvalidRow []BeneficiaryCodeRow) {

	BeneficiaryCodeTableValidRow = filterPointer(beneficiaryCode, isValidRowFormat)
	BeneficiaryCodeTableInvalidRow = filterPointer(beneficiaryCode, isInvalidRowFormat)

	// add error message to BeneficiaryCodeTableInvalidRow
	for i := 0; i < len(BeneficiaryCodeTableInvalidRow); i++ {

		BeneficiaryCodeTableInvalidRow[i].Err = BeneficiaryCodeTableInvalidRow[i].validateRowFormat().Error()
	}

	return BeneficiaryCodeTableValidRow, BeneficiaryCodeTableInvalidRow

}

// filter an array of BeneficiaryCodeRow with pointer
func filterPointer(unfilteredTable []BeneficiaryCodeRow, test func(*BeneficiaryCodeRow) bool) (filteredTable []BeneficiaryCodeRow) {
	for _, row := range unfilteredTable {
		if test(&row) {
			filteredTable = append(filteredTable, row)
		}
	}
	return
}

// check if BeneficiaryCodeRow has valid format
func isValidRowFormat(row *BeneficiaryCodeRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return false
	}
	return true

}

// check if SellerRejectionRow has invalid format
func isInvalidRowFormat(row *BeneficiaryCodeRow) bool {

	err := row.validateRowFormat()
	if err != nil {
		return true
	}
	return false

}
