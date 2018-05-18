package sqliteinteract

import (
	"database/sql"
	"log"
	"time"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

// USE MAPBENEFICIARYCODE AND GROUPFILTER TO REPRODUCE THE LOGIC OF R
// FIRST, LOOK AT COMMISSION PROCESS TO FIGURE OUT HOW TO BETTER CALCULATE commission_revenue_and_vat
// WHICH IS BULLSHIT

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

// ReturnItemPriceAndCreditTableForValidation unions the SQLite tables item_price_credit_oms & item_price_oms
// and outputs them into an array of ScOmsRow: itemPriceAndCreditTableForValidation
// which is used to check if any ledger is missing in BAA database
func ReturnItemPriceAndCreditTableForValidation(db *sql.DB) []scomsrow.ScOmsRow {

	query := `
	SELECT 
	ipco.oms_id_sales_order_item
	,ipco.order_nr
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
	FROM item_price_oms ipto
`
	var orderNr, shortCode, supplierName, transactionType, comment, itemStatus, paymentMethod, shipmentProvidername, ledgerMapKey string
	var omsIDSalesOrderItem, iDTransactionType int
	var transactionValue, paidPrice float32
	var itemPriceAndCreditTableForValidation []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &orderNr, &shortCode, &supplierName, &iDTransactionType, &transactionType, &transactionValue, &comment, &itemStatus, &paymentMethod, &shipmentProvidername, &paidPrice, &ledgerMapKey)
		checkError(err)
		itemPriceAndCreditTableForValidation = append(itemPriceAndCreditTableForValidation,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem:  omsIDSalesOrderItem,
				OrderNr:              orderNr,
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
	}

	return itemPriceAndCreditTableForValidation
}

func CreateItemPriceCreditValidTable(db *sql.DB, itemPriceAndCreditTableValid []scomsrow.ScOmsRow) {

	// create item_price_credit_valid table
	createItemPriceCreditValidTableStr := `CREATE TABLE item_price_credit_valid (
	oms_id_sales_order_item INTEGER
	,order_nr INTEGER
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
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertItemPriceCreditValidTable, err := db.Prepare(insertItemPriceCreditValidTableStr)
	checkError(err)
	for i := 0; i < len(itemPriceAndCreditTableValid); i++ {
		if itemPriceAndCreditTableValid[i].IDTransactionType == 17 {
			insertItemPriceCreditValidTable.Exec(
				itemPriceAndCreditTableValid[i].OmsIDSalesOrderItem,
				itemPriceAndCreditTableValid[i].OrderNr,
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

func CreateItemPriceValidTable(db *sql.DB, itemPriceAndCreditTableValid []scomsrow.ScOmsRow) {

	// create item_price_valid table
	createItemPriceValidTableStr := `CREATE TABLE item_price_valid (
	oms_id_sales_order_item INTEGER
	,order_nr INTEGER
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
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	insertItemPriceValidTable, err := db.Prepare(insertItemPriceValidTableStr)
	checkError(err)
	for i := 0; i < len(itemPriceAndCreditTableValid); i++ {
		if itemPriceAndCreditTableValid[i].IDTransactionType == 18 {
			insertItemPriceValidTable.Exec(
				itemPriceAndCreditTableValid[i].OmsIDSalesOrderItem,
				itemPriceAndCreditTableValid[i].OrderNr,
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

func CreateLedgerMapTable(db *sql.DB, ledgerMapTable []scomsrow.ScOmsRow) {

	// create ledger_map table
	createLedgerMapTableStr := `CREATE TABLE ledger_map (
	ledger_map_key TEXT
	,ledger INTEGER
	,subledger INTEGER)`
	createLedgerMapTable, err := db.Prepare(createLedgerMapTableStr)
	checkError(err)
	createLedgerMapTable.Exec()

	// insert values into ledger_map table
	insertLedgerMapTableStr := `INSERT INTO ledger_map (
		ledger_map_key
		,ledger
		,subledger) 
	VALUES (?, ?, ?)`
	insertLedgerMapTable, err := db.Prepare(insertLedgerMapTableStr)
	checkError(err)
	for i := 0; i < len(ledgerMapTable); i++ {

		insertLedgerMapTable.Exec(
			ledgerMapTable[i].LedgerMapKey,
			ledgerMapTable[i].Ledger,
			ledgerMapTable[i].Subledger,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

func CreateBeneficiaryCodeTable(db *sql.DB, beneficiaryCodeTable []scomsrow.ScOmsRow) {

	// create beneficiary_code_map table
	createBeneficiaryCodeTableStr := `CREATE TABLE beneficiary_code_map (
	short_code TEXT
	,beneficiary_code INTEGER)`
	createBeneficiaryCodeTable, err := db.Prepare(createBeneficiaryCodeTableStr)
	checkError(err)
	createBeneficiaryCodeTable.Exec()

	// insert values into beneficiary_code_map table
	insertBeneficiaryCodeTableStr := `INSERT INTO beneficiary_code_map (
		short_code
		,beneficiary_code) 
	VALUES (?, ?)`
	insertBeneficiaryCodeTable, err := db.Prepare(insertBeneficiaryCodeTableStr)
	checkError(err)
	for i := 0; i < len(beneficiaryCodeTable); i++ {

		insertBeneficiaryCodeTable.Exec(
			beneficiaryCodeTable[i].ShortCode,
			beneficiaryCodeTable[i].BeneficiaryCode,
		)
		time.Sleep(1 * time.Millisecond)
	}

}

func JoinLedgerMapToItemPriceCredit(db *sql.DB) {

	createIpcLedgerViewStr := `
	CREATE VIEW ipc_ledger AS
	SELECT 
	ipcv.oms_id_sales_order_item
		,ipcv.order_nr
		,ipcv.short_code
		,ipcv.supplier_name
		,ipcv.id_transaction_type
		,ipcv.transaction_type
		,ipcv.transaction_value
		,ipcv.comment
		,ipcv.item_status
		,ipcv.payment_method
		,ipcv.shipment_provider_name
		,ipcv.paid_price
		,ipcv.transaction_value - ipcv.paid_price 'voucher'
		,lm.ledger
		,lm.subledger
	FROM item_price_credit_valid ipcv 
	LEFT JOIN ledger_map lm 
	USING(ledger_map_key)
	`

	createIpcLedgerView, err := db.Prepare(createIpcLedgerViewStr)
	checkError(err)
	createIpcLedgerView.Exec()

}

func JoinLedgerMapToItemPrice(db *sql.DB) {

	createIptLedgerViewStr := `
	CREATE VIEW ipt_ledger AS
	SELECT 
	iptv.oms_id_sales_order_item
		,iptv.order_nr
		,iptv.short_code
		,iptv.supplier_name
		,iptv.id_transaction_type
		,iptv.transaction_type
		,iptv.transaction_value
		,iptv.comment
		,iptv.item_status
		,iptv.payment_method
		,iptv.shipment_provider_name
		,iptv.paid_price * (-1) 'paid_price'
		,iptv.transaction_value - iptv.paid_price 'voucher'
		,lm.ledger
		,lm.subledger
	FROM item_price_valid iptv 
	LEFT JOIN ledger_map lm 
	USING(ledger_map_key)
	`

	createIptLedgerView, err := db.Prepare(createIptLedgerViewStr)
	checkError(err)
	createIptLedgerView.Exec()

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

func filterGroup(db *sql.DB, inputTable, ledgerFilter, groupBy, sumBy, sign string) {

	// store the query in a string
	filterGroupStr := `
CREATE VIEW ` + inputTable + `_fg AS
SELECT 
,` + inputTable + `.` + sumBy + ` * (` + sign + `) 'amount'
,` + inputTable + `.ledger
,` + inputTable + `.subledger
FROM ` + inputTable +
		` WHERE ` + inputTable + `.ledger IN(` + ledgerFilter + `)
		GROUP BY ` + groupBy

	filterGroup, err := db.Prepare(filterGroupStr)
	checkError(err)
	filterGroup.Exec()
}

func mapBeneficiaryCode(db *sql.DB, inputTable string) {

	// store the query in a string
	mapBeneficiaryCodeStr := `
CREATE VIEW ` + inputTable + `_bc AS
SELECT 
,` + inputTable + `.transaction_value
,` + inputTable + `.paid_price
,` + inputTable + `.voucher
,` + inputTable + `.ledger
,bcm.beneficiary_code 'subledger'
FROM ` + inputTable +
		` LEFT JOIN beneficiary_code_map bcm
		USING(short_code) `

	mapBeneficiaryCode, err := db.Prepare(mapBeneficiaryCodeStr)
	checkError(err)
	mapBeneficiaryCode.Exec()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
