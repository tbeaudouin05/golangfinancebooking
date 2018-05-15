package scomsrow

// ScOmsRow represents a row of data coming from Seller Center and OMS used in Finance Booking process
type ScOmsRow struct {
	Err                    string `json:"error"`
	IDTransaction          string `json:"id_transaction"`
	OmsIDSalesOrderItem    string `json:"oms_id_sales_order_item"`
	OrderNr                string `json:"order_nr"`
	OmsSoiCreatedAt        string `json:"oms_soi_created_at"`
	IDSupplier             string `json:"id_supplier"`
	ShortCode              string `json:"short_code"`
	SupplierName           string `json:"supplier_name"`
	IDTransactionType      string `json:"id_transaction_type"`
	TransactionValue       string `json:"transaction_value"`
	IDTransactionStatement string `json:"id_transaction_statement"`
	StatementStartDate     string `json:"statement_start_date"`
	StatementEndDate       string `json:"statement_end_date"`
	Comment                string `json:"comment"`
}
