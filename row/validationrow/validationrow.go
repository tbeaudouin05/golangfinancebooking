package validationrow

import (
	validation "github.com/go-ozzo/ozzo-validation"
)

// ValidationRow represents a row of data coming from OMS used in Finance Booking process to validate data
type ValidationRow struct {
	Err          string `json:"error"`
	LedgerMapKey string `json:"ledger_map_key"`
}

// define validation for each field of InboundIssueRow
func (row ValidationRow) validateRowFormat() error {
	return validation.ValidateStruct(&row,
		// Timestamp cannot be empty, and must be a date in format 2006/01/02 15:04:05
		validation.Field(&row.Timestamp, validation.Required, validation.Date("1/2/2006 15:04:05")),
	)
}
