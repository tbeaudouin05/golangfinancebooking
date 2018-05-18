package sqliteinteract

import (
	"database/sql"
	"log"
	"time"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

type transactionType struct {
	idTransactionType string
	transactionType   string
}

var arrayOfTransactionType = []transactionType{{
	idTransactionType: `17`, // Item Price Credit
	transactionType:   `item_price_credit`,
}, {idTransactionType: `18`, // Item Price
	transactionType: `item_price`,
}, {idTransactionType: `78,77,2`, // Commission Fee (No Discount),Commission Fee (Discounted),Commission
	transactionType: `commission`,
}, {idTransactionType: `19`, // Commission Credit
	transactionType: `commission_credit`,
}, {idTransactionType: `1`, // Shipping Fee (Order Level)
	transactionType: `shipping_fee`,
}, {idTransactionType: `4`, // Shipping Fee (Order Level) Credit
	transactionType: `shipping_fee_credit`,
}, {idTransactionType: `74`, // Cancellation Penalty - (Within 24 Hrs)
	transactionType: `cancel_penalty_wi_24`,
}, {idTransactionType: `75`, // Cancellation Penalty  (After 24 Hrs)
	transactionType: `cancel_penalty_a_24`,
}, {idTransactionType: `76`, // Consigned Order Items handling Fee
	transactionType: `consign_handling_fee`,
}, {idTransactionType: `60`, // Down Payment Credit
	transactionType: `down_payment_credit`,
}, {idTransactionType: `43`, // Lost or Damaged (Product Level) Credit
	transactionType: `lost_damaged_credit`,
}, {idTransactionType: `13`, // Storage Fee
	transactionType: `storage_fee`,
}}

// NB: I think the whole app would be much more efficient if SQLite was not used in-memory!!!

// CreateScTable creates the SQLite table sc with the data from sellerCenterTable, an array of ScOmsRow
func CreateScTable(db *sql.DB, sellerCenterTable []scomsrow.ScOmsRow) {

	// create sc table
	createScTableStr := `CREATE TABLE sc (
	oms_id_sales_order_item INTEGER
	,order_nr INTEGER
	,short_code TEXT
	,supplier_name TEXT
	,id_transaction_type INTEGER
	,transaction_type TEXT
	,transaction_value REAL
	,comment TEXT`

	createScTable, err := db.Prepare(createScTableStr)
	checkError(err)
	createScTable.Exec()

	// insert values into sc table
	insertScTableStr := `INSERT INTO sc (
		oms_id_sales_order_item
		,order_nr
		,short_code
		,supplier_name
		,id_transaction_type
		,transaction_type
		,transaction_value
		,comment)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	insertScTable, err := db.Prepare(insertScTableStr)
	checkError(err)
	for i := 0; i < len(sellerCenterTable); i++ {
		insertScTable.Exec(
			sellerCenterTable[i].OmsIDSalesOrderItem,
			sellerCenterTable[i].OrderNr,
			sellerCenterTable[i].ShortCode,
			sellerCenterTable[i].SupplierName,
			sellerCenterTable[i].IDTransactionType,
			sellerCenterTable[i].TransactionType,
			sellerCenterTable[i].Comment,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

// CreateOmsTableItemPrice creates the SQLite table oms with the data from omsTable, an array of ScOmsRow
// this table is only used in Item Price and Item Price Credit processes
func CreateOmsTableItemPrice(db *sql.DB, omsTable []scomsrow.ScOmsRow) {

	// create oms table
	createOmsTableStr := `CREATE TABLE oms (
		oms_id_sales_order_item INTEGER
		,item_status TEXT
		,payment_method TEXT
		,shipment_provider_name TEXT
		,paid_price INTEGER)`

	createOmsTable, err := db.Prepare(createOmsTableStr)
	checkError(err)
	createOmsTable.Exec()

	// insert values into oms table
	insertOmsTableStr := `INSERT INTO oms (
		oms_id_sales_order_item
		,item_status
		,payment_method
		,shipment_provider_name
		,paid_price) 
	VALUES (?, ?, ?, ?, ?)`
	insertOmsTable, err := db.Prepare(insertOmsTableStr)
	checkError(err)
	for i := 0; i < len(omsTable); i++ {
		insertOmsTable.Exec(
			omsTable[i].OmsIDSalesOrderItem,
			omsTable[i].ItemStatus,
			omsTable[i].PaymentMethod,
			omsTable[i].ShipmentProviderName,
			omsTable[i].PaidPrice,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

// CreateTransactionTypeTable splits sc table into transaction_type views in SQLite
// - it also splits sc table between rows with comment vs. without comment
// - it also joins oms table to item_price and item_price_credit views
func CreateTransactionTypeTable(db *sql.DB) {
	for _, transactionType := range arrayOfTransactionType {
		createTransactionTypeView(db,
			transactionType.idTransactionType,
			transactionType.transactionType)
		createTransactionTypeCommentView(db,
			transactionType.idTransactionType,
			transactionType.transactionType)
	}

	// only joins oms to item_price and item_price_credit without comment
	createItemPriceCreditOmsView(db)
	createItemPriceOmsView(db)

}

// ReturnItemPriceTableForValidation unions the SQLite tables item_price_credit_oms & item_price_oms
// and outputs them into an array of ScOmsRow: itemPriceTableForValidation
// which is used to check if any ledger is missing in BAA database
func ReturnItemPriceTableForValidation(db *sql.DB) []scomsrow.ScOmsRow {

	query := `
	SELECT 
	ipco.oms_id_sales_order_item
	,ipco.order_nr
	,ipco.short_code
	,ipco.supplier_name
	,ipco.transaction_type
	,ipco.transaction_value
	,ipco.comment
	,ipco.item_status
	,ipco.payment_method
	,ipco.shipment_provider_name
	,ipco.paid_price
	,ipco.ledger_map_key
	FROM item_price_credit_oms ipco
	UNION ALL
	SELECT 
	ipto.oms_id_sales_order_item
	,ipto.order_nr
	,ipto.short_code
	,ipto.supplier_name
	,ipto.transaction_type
	,ipto.transaction_value
	,ipto.comment
	,ipto.item_status
	,ipto.payment_method
	,ipto.shipment_provider_name
	,ipto.paid_price
	,ipto.ledger_map_key
	FROM item_price_oms ipto
`
	var orderNr, shortCode, supplierName, transactionType, comment, itemStatus, paymentMethod, shipmentProvidername, ledgerMapKey string
	var omsIDSalesOrderItem int
	var transactionValue, paidPrice float32
	var itemPriceTableForValidation []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &orderNr, &shortCode, &supplierName, &transactionType, &transactionValue, &comment, &itemStatus, &paymentMethod, &shipmentProvidername, &paidPrice, &ledgerMapKey)
		checkError(err)
		itemPriceTableForValidation = append(itemPriceTableForValidation,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem:  omsIDSalesOrderItem,
				OrderNr:              orderNr,
				ShortCode:            shortCode,
				SupplierName:         supplierName,
				TransactionType:      transactionType,
				TransactionValue:     transactionValue,
				Comment:              comment,
				ItemStatus:           itemStatus,
				PaymentMethod:        paymentMethod,
				ShipmentProviderName: shipmentProvidername,
				PaidPrice:            paidPrice,
				LedgerMapKey:         ledgerMapKey,
			})
	}

	return itemPriceTableForValidation
}

// createTransactionTypeCommentView filters sc table
// on sc.id_transaction_type = idTransactionType AND sc.comment IS NOT NULL
// to create the view transactionType_c
func createTransactionTypeCommentView(db *sql.DB, idTransactionType, transactionType string) {

	createTransactionTypeViewStr := `
	CREATE VIEW ` + transactionType + `_c AS
	SELECT 
	sc.oms_id_sales_order_item
	,sc.order_nr
	,sc.short_code
	,sc.supplier_name
	,sc.id_transaction_type
	,sc.transaction_type
	,sc.transaction_value
	,sc.comment
	FROM sc 
	WHERE sc.comment IS NOT NULL
	AND sc.id_transaction_type IN(` + idTransactionType + `)
	`

	createTransactionTypeView, err := db.Prepare(createTransactionTypeViewStr)
	checkError(err)
	createTransactionTypeView.Exec()

}

// createTransactionTypeView filters sc table
// on sc.id_transaction_type = idTransactionType AND sc.comment IS NULL
// to create the view transactionType
func createTransactionTypeView(db *sql.DB, idTransactionType, transactionType string) {

	createTransactionTypeViewStr := `
	CREATE VIEW ` + transactionType + ` AS
	SELECT 
	sc.oms_id_sales_order_item
	,sc.order_nr
	,sc.short_code
	,sc.supplier_name
	,sc.id_transaction_type
	,sc.transaction_type
	,sc.transaction_value
	,sc.comment
	FROM sc 
	WHERE sc.comment IS NULL
	AND sc.id_transaction_type IN(` + idTransactionType + `)
	`

	createTransactionTypeView, err := db.Prepare(createTransactionTypeViewStr)
	checkError(err)
	createTransactionTypeView.Exec()

}

// createItemPriceCreditOmsView creates the view item_price_credit_oms
// by joining item_price_credit view
// to oms table on oms_id_sales_order_item
func createItemPriceCreditOmsView(db *sql.DB) {

	// store the query in a string
	createItemPriceCreditOmsViewStr := `
	CREATE VIEW item_price_credit_oms AS
	SELECT 
	ipc.oms_id_sales_order_item
	,ipc.order_nr
	,ipc.short_code
	,ipc.supplier_name
	,ipc.transaction_type
	,ipc.transaction_value
	,ipc.comment
	,oms.item_status
	,oms.payment_method
	,oms.shipment_provider_name
	,oms.paid_price
	,ipc.transaction_type ||'-'|| oms.item_status ||'-'|| oms.payment_method ||'-'|| oms.shipment_provider_name 'ledger_map_key'
	FROM item_price_credit ipc LEFT JOIN oms USING(oms_id_sales_order_item)
	`

	createItemPriceCreditOmsView, err := db.Prepare(createItemPriceCreditOmsViewStr)
	checkError(err)
	createItemPriceCreditOmsView.Exec()
}

// createItemPriceOmsView creates the view item_price_oms
// by joining item_price view
// to oms table on oms_id_sales_order_item
func createItemPriceOmsView(db *sql.DB) {

	// store the query in a string
	createItemPriceOmsViewStr := `
	CREATE VIEW item_price_oms AS
	SELECT 
	ipt.oms_id_sales_order_item
	,ipt.order_nr
	,ipt.short_code
	,ipt.supplier_name
	,ipt.transaction_type
	,ipt.transaction_value
	,ipt.comment
	,oms.item_status
	,oms.payment_method
	,oms.shipment_provider_name
	,oms.paid_price
	,ipt.transaction_type ||'-'|| oms.item_status ||'-'|| oms.payment_method ||'-'|| oms.shipment_provider_name 'ledger_map_key'
	FROM item_price ipt LEFT JOIN oms USING(oms_id_sales_order_item)
	`

	createItemPriceOmsView, err := db.Prepare(createItemPriceOmsViewStr)
	checkError(err)
	createItemPriceOmsView.Exec()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
