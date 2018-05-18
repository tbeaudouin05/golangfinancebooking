package scinteract

import (
	"database/sql"
	"log"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

// GetSellerCenterData gets the Seller Center data required for Finance Booking process
func GetSellerCenterData(dbSc *sql.DB) []scomsrow.ScOmsRow {

	// store sellerCenterQuery in a string
	sellerCenterQuery := `
	SELECT 
	t.id_transaction
	,soi.src_id 'oms_id_sales_order_item'
	,so.order_nr
	,s.id_seller
	,s.short_code
	,s.name AS  'seller_name'
	,tasg.id_tre2_account_statement_group 'id_transaction_type'
	,t.value AS  'transaction_value'
	,ts.id_transaction_statement
	,ts.start_date AS 'statement_start_date'
	,ts.end_date AS 'statement_end_date'
	,t.description AS 'comment'
	
  
	FROM transaction t 
  
	LEFT JOIN tre2_account_statement_group tasg
	ON t.fk_tre2_account_statement_group = tasg.id_tre2_account_statement_group
  
	LEFT JOIN seller s
	ON t.fk_seller = s.id_seller
	
	LEFT JOIN sales_order_item soi
	ON soi.id_sales_order_item = t.ref
  
	LEFT JOIN sales_order so
	ON so.id_sales_order = soi.fk_sales_order
  
	LEFT JOIN shipment_provider sp
	ON sp.id_shipment_provider = soi.fk_shipment_provider
  
	LEFT JOIN transaction_statement ts
	ON ts.id_transaction_statement = t.fk_transaction_statement

	WHERE MONTH(t.created_at) = CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN 12 ELSE MONTH(CURRENT_DATE())-1 END
	AND YEAR(t.created_at) = CASE WHEN MONTH(CURRENT_DATE()) = 1 THEN YEAR(CURRENT_DATE())-1 ELSE YEAR(CURRENT_DATE()) END`

	// write sellerCenterQuery result to an array of scomsrow.ScOmsRow, this array of rows represents sellerCenterTable
	var orderNr, shortCode, supplierName, statementStartDate, statementEndDate, comment string
	var iDTransaction, omsIDSalesOrderItem, iDSupplier, iDTransactionStatement, iDTransactionType int
	var transactionValue float32
	var sellerCenterTable []scomsrow.ScOmsRow

	rows, _ := dbSc.Query(sellerCenterQuery)

	for rows.Next() {
		err := rows.Scan(&iDTransaction, &omsIDSalesOrderItem, &orderNr, &iDSupplier, &shortCode, &supplierName, &iDTransactionType, &transactionValue, &iDTransactionStatement, &statementStartDate, &statementEndDate, &comment)
		checkError(err)
		sellerCenterTable = append(sellerCenterTable,
			scomsrow.ScOmsRow{
				IDTransaction:          iDTransaction,
				OmsIDSalesOrderItem:    omsIDSalesOrderItem,
				OrderNr:                orderNr,
				IDSupplier:             iDSupplier,
				ShortCode:              shortCode,
				SupplierName:           supplierName,
				IDTransactionType:      iDTransactionType,
				TransactionValue:       transactionValue,
				IDTransactionStatement: iDTransactionStatement,
				StatementStartDate:     statementStartDate,
				StatementEndDate:       statementEndDate,
				Comment:                comment,
			})
	}

	return sellerCenterTable
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
