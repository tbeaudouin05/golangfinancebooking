package validate

import (
	"database/sql"
	"log"
	"time"

	"github.com/joho/sqltocsv"
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
	,id_supplier INTEGER
	,short_code TEXT
	,supplier_name TEXT
	,id_transaction_type INTEGER
	,transaction_type TEXT
	,transaction_value REAL
	,comment TEXT)`

	createScTable, err := db.Prepare(createScTableStr)
	checkError(err)
	createScTable.Exec()

	// insert values into sc table
	insertScTableStr := `INSERT INTO sc (
		oms_id_sales_order_item
		,order_nr
		,id_supplier
		,short_code
		,supplier_name
		,id_transaction_type
		,transaction_type
		,transaction_value
		,comment)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertScTable, err := db.Prepare(insertScTableStr)
	checkError(err)
	for i := 0; i < len(sellerCenterTable); i++ {
		insertScTable.Exec(
			sellerCenterTable[i].OmsIDSalesOrderItem,
			sellerCenterTable[i].OrderNr,
			sellerCenterTable[i].IDSupplier,
			sellerCenterTable[i].ShortCode,
			sellerCenterTable[i].SupplierName,
			sellerCenterTable[i].IDTransactionType,
			sellerCenterTable[i].TransactionType,
			sellerCenterTable[i].TransactionValue,
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

// ReturnItemPriceAndCreditTableForValidation unions the SQLite tables item_price_credit_oms & item_price_oms
// and outputs them into an array of ScOmsRow: itemPriceAndCreditTableForValidation
// which is used to check if (i) any ledger is missing in BAA database and (ii) all rows of item_price_credit_oms & item_price_oms are valid
func ReturnItemPriceAndCreditTableForValidation(db *sql.DB) []scomsrow.ScOmsRow {

	query := `
	SELECT 
	ipco.oms_id_sales_order_item
	,ipco.order_nr
	,ipco.id_supplier
	,ipco.short_code
	,ipco.supplier_name
	,ipco.id_transaction_type
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
	,ipto.id_supplier
	,ipto.short_code
	,ipto.supplier_name
	,ipto.id_transaction_type
	,ipto.transaction_type
	,ipto.transaction_value
	,ipto.comment
	,ipto.item_status
	,ipto.payment_method
	,ipto.shipment_provider_name
	,ipto.paid_price
	,ipto.ledger_map_key
	FROM  item_price_oms ipto
`
	var orderNr, shortCode, supplierName, transactionType, comment, itemStatus, paymentMethod, shipmentProvidername, ledgerMapKey string
	var omsIDSalesOrderItem, iDSupplier, iDTransactionType int
	var transactionValue, paidPrice float32
	var itemPriceAndCreditTableForValidation []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &orderNr, &iDSupplier, &shortCode, &supplierName, &iDTransactionType, &transactionType, &transactionValue, &comment, &itemStatus, &paymentMethod, &shipmentProvidername, &paidPrice, &ledgerMapKey)
		checkError(err)
		itemPriceAndCreditTableForValidation = append(itemPriceAndCreditTableForValidation,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem:  omsIDSalesOrderItem,
				OrderNr:              orderNr,
				IDSupplier:           iDSupplier,
				ShortCode:            shortCode,
				SupplierName:         supplierName,
				IDTransactionType:    iDTransactionType,
				TransactionType:      transactionType,
				TransactionValue:     transactionValue,
				Comment:              comment,
				ItemStatus:           itemStatus,
				PaymentMethod:        paymentMethod,
				ShipmentProviderName: shipmentProvidername,
				PaidPrice:            paidPrice,
				LedgerMapKey:         ledgerMapKey,
			})

		//err = sqltocsv.WriteFile("itemPriceAndCreditTableForValidation.csv", rows)
		//checkError(err)
	}

	return itemPriceAndCreditTableForValidation
}

// CreateItemPriceCreditValidTable creates item_price_credit_valid from itemPriceAndCreditTableValid
func CreateItemPriceCreditValidTable(db *sql.DB, itemPriceAndCreditTableValid []scomsrow.ScOmsRow) {

	// create item_price_credit_valid table
	createItemPriceCreditValidTableStr := `CREATE TABLE item_price_credit_valid (
	oms_id_sales_order_item INTEGER
	,order_nr INTEGER
	,id_supplier INTEGER
	,short_code TEXT
	,supplier_name TEXT
	,id_transaction_type INTEGER
	,transaction_type TEXT
	,transaction_value REAL
	,comment TEXT
	,item_status TEXT
	,payment_method TEXT
	,shipment_provider_name TEXT
	,paid_price REAL
	,ledger_map_key TEXT)`

	createItemPriceCreditValidTable, err := db.Prepare(createItemPriceCreditValidTableStr)
	checkError(err)
	createItemPriceCreditValidTable.Exec()

	// insert values into item_price_credit_valid table
	insertItemPriceCreditValidTableStr := `INSERT INTO item_price_credit_valid (
		oms_id_sales_order_item
		,order_nr
		,id_supplier
		,short_code
		,supplier_name
		,id_transaction_type
		,transaction_type
		,transaction_value
		,comment
		,item_status
		,payment_method
		,shipment_provider_name
		,paid_price
		,ledger_map_key) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertItemPriceCreditValidTable, err := db.Prepare(insertItemPriceCreditValidTableStr)
	checkError(err)
	for i := 0; i < len(itemPriceAndCreditTableValid); i++ {
		if itemPriceAndCreditTableValid[i].IDTransactionType == 17 {
			insertItemPriceCreditValidTable.Exec(
				itemPriceAndCreditTableValid[i].OmsIDSalesOrderItem,
				itemPriceAndCreditTableValid[i].OrderNr,
				itemPriceAndCreditTableValid[i].IDSupplier,
				itemPriceAndCreditTableValid[i].ShortCode,
				itemPriceAndCreditTableValid[i].SupplierName,
				itemPriceAndCreditTableValid[i].IDTransactionType,
				itemPriceAndCreditTableValid[i].TransactionType,
				itemPriceAndCreditTableValid[i].TransactionValue,
				itemPriceAndCreditTableValid[i].Comment,
				itemPriceAndCreditTableValid[i].ItemStatus,
				itemPriceAndCreditTableValid[i].PaymentMethod,
				itemPriceAndCreditTableValid[i].ShipmentProviderName,
				itemPriceAndCreditTableValid[i].PaidPrice,
				itemPriceAndCreditTableValid[i].LedgerMapKey,
			)
			time.Sleep(1 * time.Millisecond)
		}
	}

}

// CreateItemPriceValidTable creates item_price_valid from itemPriceAndCreditTableValid
func CreateItemPriceValidTable(db *sql.DB, itemPriceAndCreditTableValid []scomsrow.ScOmsRow) {

	// create item_price_valid table
	createItemPriceValidTableStr := `CREATE TABLE item_price_valid (
	oms_id_sales_order_item INTEGER
	,order_nr INTEGER
	,id_supplier INTEGER
	,short_code TEXT
	,supplier_name TEXT
	,id_transaction_type INTEGER
	,transaction_type TEXT
	,transaction_value REAL
	,comment TEXT
	,item_status TEXT
	,payment_method TEXT
	,shipment_provider_name TEXT
	,paid_price REAL
	,ledger_map_key TEXT)`

	createItemPriceValidTable, err := db.Prepare(createItemPriceValidTableStr)
	checkError(err)
	createItemPriceValidTable.Exec()

	// insert values into item_price_valid table
	insertItemPriceValidTableStr := `INSERT INTO item_price_valid (
		oms_id_sales_order_item
		,order_nr
		,id_supplier
		,short_code
		,supplier_name
		,id_transaction_type
		,transaction_type
		,transaction_value
		,comment
		,item_status
		,payment_method
		,shipment_provider_name
		,paid_price
		,ledger_map_key) 
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertItemPriceValidTable, err := db.Prepare(insertItemPriceValidTableStr)
	checkError(err)
	for i := 0; i < len(itemPriceAndCreditTableValid); i++ {
		if itemPriceAndCreditTableValid[i].IDTransactionType == 18 {
			insertItemPriceValidTable.Exec(
				itemPriceAndCreditTableValid[i].OmsIDSalesOrderItem,
				itemPriceAndCreditTableValid[i].OrderNr,
				itemPriceAndCreditTableValid[i].IDSupplier,
				itemPriceAndCreditTableValid[i].ShortCode,
				itemPriceAndCreditTableValid[i].SupplierName,
				itemPriceAndCreditTableValid[i].IDTransactionType,
				itemPriceAndCreditTableValid[i].TransactionType,
				itemPriceAndCreditTableValid[i].TransactionValue,
				itemPriceAndCreditTableValid[i].Comment,
				itemPriceAndCreditTableValid[i].ItemStatus,
				itemPriceAndCreditTableValid[i].PaymentMethod,
				itemPriceAndCreditTableValid[i].ShipmentProviderName,
				itemPriceAndCreditTableValid[i].PaidPrice,
				itemPriceAndCreditTableValid[i].LedgerMapKey,
			)
			time.Sleep(1 * time.Millisecond)
		}
	}

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
	,sc.id_supplier
	,sc.short_code
	,sc.supplier_name
	,sc.id_transaction_type
	,sc.transaction_type
	,sc.transaction_value
	,sc.comment
	FROM sc 
	WHERE sc.comment <> 'NULL'
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
	,sc.id_supplier
	,sc.short_code
	,sc.supplier_name
	,sc.id_transaction_type
	,sc.transaction_type
	,sc.transaction_value
	,sc.comment
	FROM sc 
	-- WHERE sc.comment = 'NULL'
	WHERE sc.id_transaction_type IN(` + idTransactionType + `)
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
	,ipc.id_supplier
	,ipc.short_code
	,ipc.supplier_name
	,ipc.id_transaction_type
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
	,ipt.id_supplier
	,ipt.short_code
	,ipt.supplier_name
	,ipt.id_transaction_type
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

func DownloadToCsvTest(db *sql.DB, tableName string) {

	query := `SELECT ` + tableName + `.oms_id_sales_order_item FROM ` + tableName
	var omsIDSalesOrderItem int
	var ngsTemplate []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem)
		checkError(err)
		ngsTemplate = append(ngsTemplate,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem: omsIDSalesOrderItem,
			})
		err = sqltocsv.WriteFile(tableName+".csv", rows)
		checkError(err)
	}

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
