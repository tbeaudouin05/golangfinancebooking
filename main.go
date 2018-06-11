package main

import (
	"log"
	"strconv"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"

	"github.com/thomas-bamilo/financebooking/dbinteract/baainteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/omsinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/scinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/sqliteinteract/output"
	"github.com/thomas-bamilo/financebooking/dbinteract/sqliteinteract/transform"
	"github.com/thomas-bamilo/financebooking/dbinteract/sqliteinteract/validate"
	"github.com/thomas-bamilo/financebooking/validation"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	// get Seller Center data
	dbSc := connectdb.ConnectToSc()
	defer dbSc.Close()
	sellerCenterTable := scinteract.GetSellerCenterData(dbSc)

	log.Println(`sellerCenterTable`)
	log.Println(`sellerCenterTable length: ` + strconv.Itoa(len(sellerCenterTable)))

	// filter out retail suppliers
	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	retailShortCodeTable := baainteract.GetRetailShortCodeFromBaa(dbBaa)
	sellerCenterTable = validation.FilterRetailShortCode(retailShortCodeTable, sellerCenterTable)
	log.Println(`retail suppliers filtered out`)
	log.Println(`sellerCenterTable length: ` + strconv.Itoa(len(sellerCenterTable)))

	// check if sellerCenterTable has any invalid row
	// if sellerCenterTable has any invalid row, send the invalid rows to Finance
	// and continue the process only with valid rows
	// FYI: this step DOES NOT remove any seller without ShortCode (e.g. Bamilo) --> you should make sure they are removed
	sellerCenterTable, sellerCenterTableInvalidRow := scomsrow.FilterSellerCenterTable(sellerCenterTable)
	log.Println(`checked seller center rows validity`)
	log.Println(`sellerCenterTable length: ` + strconv.Itoa(len(sellerCenterTable)))
	log.Println(`sellerCenterTableInvalidRow length: ` + strconv.Itoa(len(sellerCenterTableInvalidRow)))
	scomsrow.IfInvalidSellerCenterRow(sellerCenterTableInvalidRow)
	log.Println(`IfInvalidSellerCenterRow`)

	// check benef_code_map is complete ------------------------------------------------
	// get all the short_code from beneficiary_code_map table of BAA database to check against the ShortCode of sellerCenterTable
	beneficiaryCodeTable := baainteract.GetBeneficiaryCodeTable(dbBaa)
	log.Println(`beneficiaryCodeTable`)
	log.Println(`beneficiaryCodeTable length: ` + strconv.Itoa(len(beneficiaryCodeTable)))
	// check if any missing short_code in beneficairy_code_map table of BAA database
	missingBeneficiaryCodeTable := validation.MissingBeneficiaryCode(beneficiaryCodeTable, sellerCenterTable)
	log.Println(`missingBeneficiaryCodeTable`)
	log.Println(`missingBeneficiaryCodeTable length: ` + strconv.Itoa(len(missingBeneficiaryCodeTable)))

	// IfMissingBeneficiaryCode STOPs the booking process if any missing short_code in beneficairy_code_map table of BAA database
	// (we can make it less blocking in the future)
	validation.IfMissingBeneficiaryCode(missingBeneficiaryCodeTable)
	log.Println(`IfMissingBeneficiaryCode`)

	// get uniqueOmsIDSalesOrderItemList from Seller Center table
	// uniqueOmsIDSalesOrderItemList represents in OMS the rows currently booked
	uniqueOmsIDSalesOrderItemList := uniqueOmsIDSalesOrderItem(sellerCenterTable)
	log.Println(`uniqueOmsIDSalesOrderItemList`)
	//log.Println(uniqueOmsIDSalesOrderItemList)

	// get OMS data
	dbOMS := connectdb.ConnectToOms()
	defer dbOMS.Close()
	omsTable := omsinteract.GetOmsData(dbOMS, uniqueOmsIDSalesOrderItemList)
	log.Println(`GotOmsData`)
	log.Println(`omsTable length: ` + strconv.Itoa(len(omsTable)))

	// split sellerCenterTable by IDTransaction in SQLite------------------------------------------
	dbSqlite := connectdb.ConnectToSQLite()
	defer dbSqlite.Close()
	// create sc table in SQLite
	validate.CreateScTable(dbSqlite, sellerCenterTable)
	log.Println(`CreatedScTable`)
	validate.DownloadToCsvTest(dbSqlite, `sc`)
	// create oms table in SQLite
	validate.CreateOmsTableItemPrice(dbSqlite, omsTable)
	log.Println(`CreatedOmsTableItemPrice`)
	validate.DownloadToCsvTest(dbSqlite, `oms`)
	// CreateTransactionTypeTable splits sc table into transaction_type views in SQLite
	// - it also splits sc table between rows with comment vs. without comment
	// - it also joins oms table to item_price and item_price_credit views without comment
	validate.CreateTransactionTypeTable(dbSqlite)
	log.Println(`CreatedTransactionTypeTable`)
	/*validate.DownloadToCsvTest(dbSqlite, `item_price_credit`)
	validate.DownloadToCsvTest(dbSqlite, `item_price`)
	validate.DownloadToCsvTest(dbSqlite, `commission`)
	validate.DownloadToCsvTest(dbSqlite, `commission_credit`)
	validate.DownloadToCsvTest(dbSqlite, `shipping_fee`)
	validate.DownloadToCsvTest(dbSqlite, `shipping_fee_credit`)
	validate.DownloadToCsvTest(dbSqlite, `cancel_penalty_wi_24`)
	validate.DownloadToCsvTest(dbSqlite, `cancel_penalty_a_24`)
	validate.DownloadToCsvTest(dbSqlite, `consign_handling_fee`)
	validate.DownloadToCsvTest(dbSqlite, `down_payment_credit`)
	validate.DownloadToCsvTest(dbSqlite, `lost_damaged_credit`)
	validate.DownloadToCsvTest(dbSqlite, `storage_fee`)*/

	// check if item_price_oms and item_price_credit_oms have invalid rows-----------------------------------------------------------------
	// mostly, rows should not have missing values for fields involved in ledger mapping
	// return itemPriceAndCreditTableForValidation to check if any invalid row
	itemPriceAndCreditTableForValidation := validate.ReturnItemPriceAndCreditTableForValidation(dbSqlite)
	log.Println(`ReturnedItemPriceAndCreditTableForValidation`)
	log.Println(`itemPriceAndCreditTableForValidation length: ` + strconv.Itoa(len(itemPriceAndCreditTableForValidation)))

	// check if itemPriceAndCreditTableForValidation has any invalid row
	// if itemPriceAndCreditTableForValidation has any invalid row, send the invalid rows to Finance
	// and continue the process only with valid rows
	// FYI: itemPriceAndCreditTableForValidation is the union of item_price_oms and item_price_credit_oms SQLite views
	itemPriceAndCreditTableForValidation, itemPriceAndCreditTableForValidationInvalidRow := scomsrow.FilterScOmsTable(itemPriceAndCreditTableForValidation)
	log.Println(`FilteredScOmsTable`)
	log.Println(`itemPriceAndCreditTableForValidation length: ` + strconv.Itoa(len(itemPriceAndCreditTableForValidation)))
	log.Println(`itemPriceAndCreditTableForValidationInvalidRow length: ` + strconv.Itoa(len(itemPriceAndCreditTableForValidationInvalidRow)))
	scomsrow.IfInvalidScOmsRow(itemPriceAndCreditTableForValidationInvalidRow)
	log.Println(`IfInvalidScOmsRow`)

	// check ledger_map is complete -----------------------------------------------------------------------------------------------------
	// get the LedgerMapKey from ledger_map table of BAA database to check against itemPriceAndCreditTableForValidation
	ledgerMapTable := baainteract.GetLedgerMap(dbBaa)
	log.Println(`GotLedgerMap`)
	log.Println(`ledgerMapTable length: ` + strconv.Itoa(len(ledgerMapTable)))

	// check if any missing ledger_map in ledger_map table of BAA database compared to itemPriceAndCreditTableForValidation
	missingLedgerMapKeyTable := validation.MissingLedgerMapKey(ledgerMapTable, itemPriceAndCreditTableForValidation)
	log.Println(`missingLedgerMapKeyTable`)
	log.Println(`missingLedgerMapKeyTable length: ` + strconv.Itoa(len(missingLedgerMapKeyTable)))

	// IfMissingLedgerMap STOPs the booking process if any missing ledger_map in ledger_map table of BAA database compared to itemPriceAndCreditTableForValidation
	// (we can make it less blocking in the future)
	validation.IfMissingLedgerMap(missingLedgerMapKeyTable)
	log.Println(`IfMissingLedgerMap`)

	// Create item_price_credit_valid and item_price_valid SQLite tables
	validate.CreateItemPriceCreditValidTable(dbSqlite, itemPriceAndCreditTableForValidation)
	log.Println(`CreateItemPriceCreditValidTable`)
	validate.DownloadToCsvTest(dbSqlite, `item_price_credit_valid`)
	validate.CreateItemPriceValidTable(dbSqlite, itemPriceAndCreditTableForValidation)
	log.Println(`CreateItemPriceValidTable`)
	validate.DownloadToCsvTest(dbSqlite, `item_price_valid`)

	// transform valid ipc, ipt and commission data ---------------------------------------------------------------------------------------------------

	// Create ledger_map SQLite table
	transform.CreateLedgerMapTable(dbSqlite, ledgerMapTable)
	log.Println(`CreateLedgerMapTable`)
	// Create beneficiary_code_map SQLite table
	transform.CreateBeneficiaryCodeTable(dbSqlite, beneficiaryCodeTable)
	log.Println(`CreateBeneficiaryCodeTable`)

	// ipc_ipt_c process ---------------------------------------------------------------------------------------------------------------
	// add all necessary data by joining tables and adding calculated fields
	transform.CreateIpcFinal(dbSqlite)
	log.Println(`CreateIpcFinal`)
	transform.DownloadIpcIptToCsv(dbSqlite, `ipc_final`)
	transform.CreateIptFinal(dbSqlite)
	log.Println(`CreateIptFinal`)
	transform.DownloadIpcIptToCsv(dbSqlite, `ipt_final`)
	transform.CreateCommissionFinal(dbSqlite)
	log.Println(`CreateCommissionFinal`)
	transform.DownloadCommissionToCsv(dbSqlite, `commission_final`)

	// create all the "ngs-friendly" data tables
	output.CreateVoucherLedgerAmountView(dbSqlite)
	log.Println(`CreateVoucherLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `voucher_ledger_amount`)
	output.CreateIpcPaidPriceLedgerAmountView(dbSqlite)
	log.Println(`CreateIpcPaidPriceLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `ipc_paid_price_ledger_amount`)
	output.CreateIptPaidPriceLedgerAmountView(dbSqlite)
	log.Println(`CreateIptPaidPriceLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `ipt_paid_price_ledger_amount`)
	output.CreateCommissionVatLedgerAmountView(dbSqlite)
	log.Println(`CreateCommissionVatLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `commission_vat_ledger_amount`)
	output.CreateCommissionRevenueLedgerAmountView(dbSqlite)
	log.Println(`CreateCommissionRevenueLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `commission_revenue_ledger_amount`)
	output.CreateTotalLedgerAmountView(dbSqlite)
	log.Println(`CreateTotalLedgerAmountView`)
	output.DownloadToCsvTest(dbSqlite, `total_ledger_amount`)

	validate.DownloadToCsvTest(dbSqlite, `item_price_oms`)
	validate.DownloadToCsvTest(dbSqlite, `item_price_credit_oms`)

	// output ngsIpcIptC template
	output.ReturnNgsIpcIptC(dbSqlite)
	log.Println(`ReturnNgsIpcIptC`)

}

func uniqueOmsIDSalesOrderItem(sellerCenterTable []scomsrow.ScOmsRow) (uniqueOmsIDSalesOrderItemList string) {

	// initialize uniqueOmsIDSalesOrderItemMap with the first omsIDSalesOrderItem of sellerCenterTable
	uniqueOmsIDSalesOrderItemMap := make(map[int]bool)
	uniqueOmsIDSalesOrderItemMap[sellerCenterTable[0].OmsIDSalesOrderItem] = true
	// initialize uniqueOmsIDSalesOrderItemList with the first omsIDSalesOrderItem of sellerCenterTable
	uniqueOmsIDSalesOrderItemList = strconv.Itoa(sellerCenterTable[0].OmsIDSalesOrderItem)
	// for each sellerCenterRow in sellerCenterTable
	// check if there is already sellerCenterRow.omsIDSalesOrderItem in uniqueOmsIDSalesOrderItemMap
	// if yes, then do nothing
	// if no then add the omsIDSalesOrderItem to uniqueOmsIDSalesOrderItemMap and to uniqueOmsIDSalesOrderItemList
	for _, sellerCenterRow := range sellerCenterTable {
		if _, ok := uniqueOmsIDSalesOrderItemMap[sellerCenterRow.OmsIDSalesOrderItem]; !ok {
			uniqueOmsIDSalesOrderItemMap[sellerCenterRow.OmsIDSalesOrderItem] = true
			uniqueOmsIDSalesOrderItemList += `,` + strconv.Itoa(sellerCenterRow.OmsIDSalesOrderItem)
		}

	}
	return uniqueOmsIDSalesOrderItemList

}
