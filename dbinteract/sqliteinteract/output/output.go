package output

import (
	"database/sql"
	"log"

	"github.com/joho/sqltocsv"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"
)

const ledgerBookedAtSubledgerLevel = `'13004','33001','31006','33002','84006','32021','94001'`

// CreateVoucherLedgerAmountView unions ipc_final and ipt_final,
// defines Account Code (62002), Account Free (null) and Amount (voucher)
// to create voucher_ledger_amount SQLite table
func CreateVoucherLedgerAmountView(db *sql.DB) {

	createVoucherLedgerAmountViewStr := `
	CREATE VIEW voucher_ledger_amount AS
	SELECT 
		vla.'Account Code'
		,vla.'Account Free'
		,SUM(vla.Amount) 'Amount'
	FROM
	(SELECT 
		62002 'Account Code'
		,NULL 'Account Free'
		,ipc_final.voucher 'Amount'
	FROM ipc_final
	UNION ALL
	SELECT 
		62002 'Account Code'
		,NULL 'Account Free'
		,ipt_final.voucher 'Amount'
	FROM ipt_final) vla
	GROUP BY vla.'Account Code', vla.'Account Free'
	`

	createVoucherLedgerAmountView, err := db.Prepare(createVoucherLedgerAmountViewStr)
	checkError(err)
	createVoucherLedgerAmountView.Exec()

}

// CreateIpcPaidPriceLedgerAmountView defines:
// Account Code (ipc_final.ledger), Account Free (ipc_final.subledger) and Amount (ipc_final.paid_price)
// to create ipc_paid_price_ledger_amount SQLite table
func CreateIpcPaidPriceLedgerAmountView(db *sql.DB) {

	createIpcPaidPriceLedgerAmountViewStr := `
	CREATE VIEW ipc_paid_price_ledger_amount AS
	SELECT 
		ipc_final.ledger 'Account Code'
		,ipc_final.subledger 'Account Free'
		,ipc_final.paid_price 'Amount'
	FROM ipc_final
	WHERE ipc_final.ledger IN(` + ledgerBookedAtSubledgerLevel + `)
	GROUP BY ipc_final.ledger, ipc_final.subledger
	`

	createIpcPaidPriceLedgerAmountView, err := db.Prepare(createIpcPaidPriceLedgerAmountViewStr)
	checkError(err)
	createIpcPaidPriceLedgerAmountView.Exec()

}

// CreateIptPaidPriceLedgerAmountView defines:
// Account Code (ipt_final.ledger), Account Free (ipt_final.subledger) and Amount (ipt_final.paid_price)
// to create ipt_paid_price_ledger_amount SQLite table
func CreateIptPaidPriceLedgerAmountView(db *sql.DB) {

	createIptPaidPriceLedgerAmountViewStr := `
	CREATE VIEW ipt_paid_price_ledger_amount AS
	SELECT 
		ipt_final.ledger 'Account Code'
		,ipt_final.subledger 'Account Free'
		,ipt_final.paid_price 'Amount'
	FROM ipt_final
	WHERE ipt_final.ledger IN(` + ledgerBookedAtSubledgerLevel + `)
	GROUP BY ipt_final.ledger, ipt_final.subledger
	`

	createIptPaidPriceLedgerAmountView, err := db.Prepare(createIptPaidPriceLedgerAmountViewStr)
	checkError(err)
	createIptPaidPriceLedgerAmountView.Exec()

}

// CreateCommissionVatLedgerAmountView defines:
// Account Code (32021), Account Free (NULL) and Amount (commission_final.commission_vat)
// to create commission_vat_ledger_amount SQLite table
func CreateCommissionVatLedgerAmountView(db *sql.DB) {

	createCommissionVatLedgerAmountViewStr := `
	CREATE VIEW commission_vat_ledger_amount AS
	SELECT 
	cf.'Account Code'
	,cf.'Account Free'
	,SUM(cf.Amount) 'Amount'
	FROM 
	(SELECT 
		32021 'Account Code'
		,NULL 'Account Free'
		,commission_final.commission_vat 'Amount'
	FROM commission_final) cf
	GROUP BY cf.'Account Code', cf.'Account Free'
	`

	createCommissionVatLedgerAmountView, err := db.Prepare(createCommissionVatLedgerAmountViewStr)
	checkError(err)
	createCommissionVatLedgerAmountView.Exec()

}

// CreateCommissionRevenueLedgerAmountView defines:
// Account Code (62001), Account Free (commission_final.beneficiary_code) and Amount (commission_final.commission_revenue)
// to create commission_revenue_ledger_amount SQLite table
func CreateCommissionRevenueLedgerAmountView(db *sql.DB) {

	createCommissionRevenueLedgerAmountViewStr := `
	CREATE VIEW commission_revenue_ledger_amount AS
	SELECT 
	cr.'Account Code'
	,cr.'Account Free'
	,SUM(cr.'Amount') 'Amount'
	FROM 
	(SELECT 
		62001 'Account Code'
		,commission_final.beneficiary_code 'Account Free'
		,commission_final.commission_revenue 'Amount'
	FROM commission_final) cr
	GROUP BY cr.'Account Code', cr.'Account Free'
	`

	createCommissionRevenueLedgerAmountView, err := db.Prepare(createCommissionRevenueLedgerAmountViewStr)
	checkError(err)
	createCommissionRevenueLedgerAmountView.Exec()

}

// UNDERSTAND THIS BLANK LEDGER STUFF, MAYBE WE NEED TO STILL RECORD VOUCHERS EVEN IF NO LEDGER MAP
// WARNING!! IF DIFFERENCE IN VOUCHER AMOUNT BETWEEN THIS AND R THEN CHECK THIS

// CreateTotalLedgerAmountView unions:
// ipc_voucher_ledger_amount, ipt_voucher_ledger_amount,
// ipc_paid_price_ledger_amount, ipt_paid_price_ledger_amount,
// commission_vat_ledger_amount and commission_revenue_ledger_amount;
// and defines Account Code (31002), Account Free (beneficiary_code) and Amount (depending on the table)
// to create total_ledger_amount SQLite table.
func CreateTotalLedgerAmountView(db *sql.DB) {

	// ipc_paid_price_ledger_amount: in R, no filter is used here...
	// which makes me think filtering by ledgerBookedAtSubledgerLevel is useless in the original ipc_paid_price_ledger_amount...
	// yes, I think in R it is only useful for the commission booking which is bullshit and you changed here so now no need of filter probably
	createTotalLedgerAmountViewStr := `
	CREATE VIEW total_ledger_amount AS
	SELECT 
	total.'Account Code'
	,total.'Account Free'
	,SUM(total.Amount*-1) 'Amount' -- (-1) because total + sum of amounts should = 0

	FROM (

	-- ipc_voucher_ledger_amount
	SELECT
		31002 'Account Code'
		,ipc_final.beneficiary_code 'Account Free'
		,ipc_final.voucher 'Amount'
	FROM ipc_final

	UNION ALL

	-- ipt_voucher_ledger_amount
	SELECT 
		31002 'Account Code'
		,ipt_final.beneficiary_code 'Account Free'
		,ipt_final.voucher 'Amount'
	FROM ipt_final

	UNION ALL

	-- ipc_paid_price_ledger_amount
	SELECT 
		31002 'Account Code'
		,ipc_final.beneficiary_code 'Account Free'
		,ipc_final.paid_price 'Amount'
	FROM ipc_final

	UNION ALL

	-- ipt_paid_price_ledger_amount
	SELECT 
		31002 'Account Code'
		,ipt_final.beneficiary_code 'Account Free'
		,ipt_final.paid_price 'Amount'
	FROM ipt_final

	UNION ALL

	-- commission_vat_ledger_amount
	SELECT 
		31002 'Account Code'
		,commission_final.beneficiary_code 'Account Free'
		,commission_final.commission_vat 'Amount'
	FROM commission_final

	UNION ALL

	-- commission_revenue_ledger_amount
	SELECT 
		31002 'Account Code'
		,commission_final.beneficiary_code 'Account Free'
		,commission_final.commission_revenue 'Amount'
	FROM commission_final	
	
	) total

	GROUP BY total.'Account Code', total.'Account Free'
	`

	createTotalLedgerAmountView, err := db.Prepare(createTotalLedgerAmountViewStr)
	checkError(err)
	createTotalLedgerAmountView.Exec()
}

func DownloadToCsvTest(db *sql.DB, tableName string) {

	query := `SELECT 
	COALESCE(` + tableName + `.'Account Code','') 'Account Code',
	COALESCE(` + tableName + `.'Account Free','') 'Account Free',
	COALESCE(` + tableName + `.'Amount','') 'Amount'
	FROM ` + tableName
	var accountCode, accountFree, amount string
	var ngsTemplate []scomsrow.NgsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&accountCode, &accountFree, &amount)
		checkError(err)
		ngsTemplate = append(ngsTemplate,
			scomsrow.NgsRow{
				AccountCode: accountCode,
				AccountFree: accountFree,
				Amount:      amount,
			})
		err = sqltocsv.WriteFile(tableName+".csv", rows)
		checkError(err)
	}

}

// ReturnNgsIpcIptC
func ReturnNgsIpcIptC(db *sql.DB) {

	query := `
		SELECT 
		COALESCE(vla.'Account Code','') 'Account Code'
		,COALESCE(vla.'Account Free','') 'Account Free'
		,COALESCE(vla.'Amount','') 'Amount'
		FROM voucher_ledger_amount vla
		UNION ALL
		SELECT
		COALESCE(ippla.'Account Code','') 'Account Code'
		,COALESCE(ippla.'Account Free','') 'Account Free'
		,COALESCE(ippla.'Amount','') 'Amount'
		FROM ipc_paid_price_ledger_amount ippla
		UNION ALL
		SELECT 
		COALESCE(ipplab.'Account Code','') 'Account Code'
		,COALESCE(ipplab.'Account Free','') 'Account Free'
		,COALESCE(ipplab.'Amount' ,'') 'Amount'
		FROM ipt_paid_price_ledger_amount ipplab
		UNION ALL
		SELECT
		COALESCE(cvla.'Account Code','') 'Account Code'
		,COALESCE(cvla.'Account Free','') 'Account Free'
		,COALESCE(cvla.'Amount' ,'') 'Amount'
		FROM commission_vat_ledger_amount cvla
		UNION ALL
		SELECT
		COALESCE(crla.'Account Code','') 'Account Code'
		,COALESCE(crla.'Account Free','') 'Account Free'
		,COALESCE(crla.'Amount','') 'Amount'
		FROM commission_revenue_ledger_amount crla
		UNION ALL
		SELECT
		COALESCE(tla.'Account Code','') 'Account Code'
		,COALESCE(tla.'Account Free','') 'Account Free'
		,COALESCE(tla.'Amount','') 'Amount'
		FROM total_ledger_amount tla
	`
	var accountCode, accountFree, amount string
	var ngsTemplate []scomsrow.NgsRow

	rows, err := db.Query(query)
	checkError(err)

	for rows.Next() {
		err := rows.Scan(&accountCode, &accountFree, &amount)
		checkError(err)
		ngsTemplate = append(ngsTemplate,
			scomsrow.NgsRow{
				AccountCode: accountCode,
				AccountFree: accountFree,
				Amount:      amount,
			})
		err = sqltocsv.WriteFile("ngsTemplateIpcIptC.csv", rows)
		checkError(err)
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}
