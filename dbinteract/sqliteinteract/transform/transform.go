package transform

import (
	"database/sql"
	"log"
	"time"

	"github.com/joho/sqltocsv"
	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

// CreateLedgerMapTable creates the SQLite table ledger_map from ledgerMapTable
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

// CreateBeneficiaryCodeTable creates the SQLite table beneficiary_code_map from beneficiaryCodeTable
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

// CreateIpcFinal adds ledger, subledger, beneficiary_code and voucher
// to item_price_credit_valid table
func CreateIpcFinal(db *sql.DB) {

	createIpcFinalViewStr := `
	CREATE VIEW ipc_final AS
	SELECT 
	ipcv.oms_id_sales_order_item
		,ipcv.order_nr
		,ipcv.id_supplier
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
		,bcm.beneficiary_code 
	FROM item_price_credit_valid ipcv 
	LEFT JOIN ledger_map lm 
	USING(ledger_map_key)
	LEFT JOIN beneficiary_code_map bcm
	USING(short_code)
	`

	createIpcFinalView, err := db.Prepare(createIpcFinalViewStr)
	checkError(err)
	createIpcFinalView.Exec()

}

// CreateIptFinal adds ledger, subledger, beneficiary_code and voucher
// to item_price_valid table
func CreateIptFinal(db *sql.DB) {

	// iptv.transaction_value + iptv.paid_price 'voucher': + is correct
	// iptv.paid_price * (-1): (-1) is correct
	createIptFinalViewStr := `
	CREATE VIEW ipt_final AS
	SELECT 
	iptv.oms_id_sales_order_item
		,iptv.order_nr
		,iptv.id_supplier
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
		,iptv.transaction_value + iptv.paid_price 'voucher'
		,lm.ledger 
		,lm.subledger 
		,bcm.beneficiary_code
	FROM item_price_valid iptv 
	LEFT JOIN ledger_map lm 
	USING(ledger_map_key)
	LEFT JOIN beneficiary_code_map bcm
	USING(short_code)
	`

	createIptFinalView, err := db.Prepare(createIptFinalViewStr)
	checkError(err)
	createIptFinalView.Exec()

}

// CreateCommissionFinal unions commission and commission_credit tables;
// adds beneficiary_code, commission_revenue and commission_vat
func CreateCommissionFinal(db *sql.DB) {

	createCommissionFinalViewStr := `
	CREATE VIEW commission_final AS
	SELECT 
	commission.oms_id_sales_order_item
		,commission.order_nr
		,commission.id_supplier
		,commission.short_code
		,commission.supplier_name
		,commission.transaction_type
		,commission.transaction_value
		,(commission.transaction_value*-1*100/109) 'commission_revenue'
		,(commission.transaction_value*-1*100/109*0.09) 'commission_vat'
		,commission.comment
		,bcm.beneficiary_code
	FROM commission 
	LEFT JOIN beneficiary_code_map bcm
	USING(short_code)
	UNION ALL
	SELECT 
	commission_credit.oms_id_sales_order_item
		,commission_credit.order_nr
		,commission_credit.id_supplier
		,commission_credit.short_code
		,commission_credit.supplier_name
		,commission_credit.transaction_type
		,commission_credit.transaction_value
		,(commission_credit.transaction_value*-1*100/109) 'commission_revenue'
		,(commission_credit.transaction_value*-1*100/109*0.09) 'commission_vat'
		,commission_credit.comment
		,bcm.beneficiary_code
	FROM commission_credit 
	LEFT JOIN beneficiary_code_map bcm
	USING(short_code)
	`
	//(-1) * (100/109) * 0.09)
	createCommissionFinalView, err := db.Prepare(createCommissionFinalViewStr)
	checkError(err)
	createCommissionFinalView.Exec()

}

func DownloadIpcIptToCsv(db *sql.DB, tableName string) {

	query := `SELECT ` +
		tableName + `.oms_id_sales_order_item,` +
		tableName + `.order_nr,` +
		tableName + `.id_supplier,` +
		tableName + `.short_code,` +
		tableName + `.supplier_name,` +
		tableName + `.transaction_type,` +
		tableName + `.transaction_value,` +
		tableName + `.comment,` +
		tableName + `.item_status,` +
		tableName + `.payment_method,` +
		tableName + `.shipment_provider_name,` +
		tableName + `.paid_price,` +
		tableName + `.voucher,` +
		tableName + `.ledger,` +
		tableName + `.subledger,` +
		tableName + `.beneficiary_code 
	FROM ` + tableName
	var orderNr, shortCode, supplierName, transactionType, comment, itemStatus, paymentMethod, shipmentProvidername string
	var omsIDSalesOrderItem, iDSupplier, ledger, subledger, beneficiaryCode int
	var transactionValue, paidPrice, voucher float32
	var ngsTemplate []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &orderNr, &iDSupplier, &shortCode, &supplierName, &transactionType, &transactionValue, &comment, &itemStatus, &paymentMethod, &shipmentProvidername, &paidPrice, &voucher, &ledger, &subledger, &beneficiaryCode)
		checkError(err)
		ngsTemplate = append(ngsTemplate,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem:  omsIDSalesOrderItem,
				OrderNr:              orderNr,
				IDSupplier:           iDSupplier,
				ShortCode:            shortCode,
				SupplierName:         supplierName,
				TransactionType:      transactionType,
				TransactionValue:     transactionValue,
				Comment:              comment,
				ItemStatus:           itemStatus,
				PaymentMethod:        paymentMethod,
				ShipmentProviderName: shipmentProvidername,
				PaidPrice:            paidPrice,
				Voucher:              voucher,
				Ledger:               ledger,
				Subledger:            subledger,
				BeneficiaryCode:      beneficiaryCode,
			})

		err = sqltocsv.WriteFile(tableName+".csv", rows)
		checkError(err)
	}

}

func DownloadCommissionToCsv(db *sql.DB, tableName string) {

	query := `SELECT ` +
		tableName + `.oms_id_sales_order_item,` +
		tableName + `.order_nr,` +
		tableName + `.id_supplier,` +
		tableName + `.short_code,` +
		tableName + `.supplier_name,` +
		tableName + `.transaction_type,` +
		tableName + `.transaction_value,` +
		tableName + `.commission_revenue,` +
		tableName + `.commission_vat,` +
		tableName + `.comment,` +
		tableName + `.beneficiary_code 
	FROM ` + tableName

	var orderNr, shortCode, supplierName, transactionType, comment string
	var omsIDSalesOrderItem, iDSupplier, beneficiaryCode int
	var transactionValue, commissionRevenue, commissionVat float32
	var ngsTemplate []scomsrow.ScOmsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&omsIDSalesOrderItem, &orderNr, &iDSupplier, &shortCode, &supplierName, &transactionType, &transactionValue, &commissionRevenue, &commissionVat, &comment, &beneficiaryCode)
		checkError(err)
		ngsTemplate = append(ngsTemplate,
			scomsrow.ScOmsRow{
				OmsIDSalesOrderItem: omsIDSalesOrderItem,
				OrderNr:             orderNr,
				IDSupplier:          iDSupplier,
				ShortCode:           shortCode,
				SupplierName:        supplierName,
				TransactionType:     transactionType,
				TransactionValue:    transactionValue,
				CommissionRevenue:   commissionRevenue,
				CommissionVat:       commissionVat,
				Comment:             comment,
				BeneficiaryCode:     beneficiaryCode,
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
