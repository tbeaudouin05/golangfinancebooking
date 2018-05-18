package main

import (
	"strconv"

	"github.com/thomas-bamilo/financebooking/row/scomsrow"

	"github.com/thomas-bamilo/financebooking/dbinteract/baainteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/omsinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/scinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/sqliteinteract"
	"github.com/thomas-bamilo/financebooking/validation"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	// get Seller Center data
	dbSc := connectdb.ConnectToSc()
	defer dbSc.Close()
	sellerCenterTable := scinteract.GetSellerCenterData(dbSc)

	// filter out retail suppliers
	dbBaa := connectdb.ConnectToBaa()
	defer dbBaa.Close()
	retailShortCodeTable := baainteract.GetRetailShortCodeFromBaa(dbBaa)
	sellerCenterTable = validation.FilterRetailShortCode(retailShortCodeTable, sellerCenterTable)

	// check if sellerCenterTable has any invalid row
	// if sellerCenterTable has any invalid row, send the invalid rows to Finance
	// and continue the process only with valid rows
	// FYI: this step also removes any seller without ShortCode (e.g. Bamilo)
	sellerCenterTable, sellerCenterTableInvalidRow := scomsrow.FilterSellerCenterTable(sellerCenterTable)
	scomsrow.IfInvalidSellerCenterRow(sellerCenterTableInvalidRow)

	// check benef_code_map is complete ------------------------------------------------
	// get all the short_code from beneficiary_code_map table of BAA database to check against the ShortCode of sellerCenterTable
	beneficiaryCodeTable := baainteract.GetBeneficiaryCodeTable(dbBaa)

	// check if any missing short_code in beneficairy_code_map table of BAA database
	missingBeneficiaryCodeTable := validation.MissingBeneficiaryCode(beneficiaryCodeTable, sellerCenterTable)

	// IfMissingBeneficiaryCode STOPs the booking process if any missing short_code in beneficairy_code_map table of BAA database
	// (we can make it less blocking in the future)
	validation.IfMissingBeneficiaryCode(missingBeneficiaryCodeTable)

	// get uniqueOmsIDSalesOrderItemList from Seller Center table
	// uniqueOmsIDSalesOrderItemList represents in OMS the rows currently booked
	uniqueOmsIDSalesOrderItemList := uniqueOmsIDSalesOrderItem(sellerCenterTable)

	// get OMS data
	dbOMS := connectdb.ConnectToOms()
	defer dbOMS.Close()
	omsTable := omsinteract.GetOmsData(dbOMS, uniqueOmsIDSalesOrderItemList)

	// split sellerCenterTable by IDTransaction in SQLite------------------------------------------
	dbSqlite := connectdb.ConnectToSQLite()
	defer dbSqlite.Close()
	// create sc table in SQLite
	sqliteinteract.CreateScTable(dbSqlite, sellerCenterTable)
	// create oms table in SQLite
	sqliteinteract.CreateOmsTableItemPrice(dbSqlite, omsTable)
	// CreateTransactionTypeTable splits sc table into transaction_type views in SQLite
	// - it also splits sc table between rows with comment vs. without comment
	// - it also joins oms table to item_price and item_price_credit views without comment
	sqliteinteract.CreateTransactionTypeTable(dbSqlite)

	// check if item_price_oms and item_price_credit_oms have invalid rows--------------------------------
	// mostly, rows should not have missing values for fields involved in ledger mapping
	// return itemPriceAndCreditTableForValidation to check if any invalid row
	itemPriceAndCreditTableForValidation := sqliteinteract.ReturnItemPriceAndCreditTableForValidation(dbSqlite)

	// check if itemPriceAndCreditTableForValidation has any invalid row
	// if itemPriceAndCreditTableForValidation has any invalid row, send the invalid rows to Finance
	// and continue the process only with valid rows
	// FYI: itemPriceAndCreditTableForValidation is the union of item_price_oms and item_price_credit_oms SQLite views
	itemPriceAndCreditTableForValidation, itemPriceAndCreditTableForValidationInvalidRow := scomsrow.FilterScOmsTable(itemPriceAndCreditTableForValidation)
	scomsrow.IfInvalidScOmsRow(itemPriceAndCreditTableForValidationInvalidRow)

	// check ledger_map is complete ------------------------------------------------
	// get the LedgerMapKey from ledger_map table of BAA database to check against itemPriceAndCreditTableForValidation
	ledgerMapTable := baainteract.GetLedgerMap(dbBaa)

	// check if any missing ledger_map in ledger_map table of BAA database compared to itemPriceAndCreditTableForValidation
	missingLedgerMapKeyTable := validation.MissingLedgerMapKey(ledgerMapTable, itemPriceAndCreditTableForValidation)

	// IfMissingLedgerMap STOPs the booking process if any missing ledger_map in ledger_map table of BAA database compared to itemPriceAndCreditTableForValidation
	// (we can make it less blocking in the future)
	validation.IfMissingLedgerMap(missingLedgerMapKeyTable)

	// Create item_price_credit_valid and item_price_valid SQLite tables
	sqliteinteract.CreateItemPriceCreditValidTable(dbSqlite, itemPriceAndCreditTableForValidation)
	sqliteinteract.CreateItemPriceValidTable(dbSqlite, itemPriceAndCreditTableForValidation)

	// Create ledger_map SQLite table
	sqliteinteract.CreateLedgerMapTable(dbSqlite, ledgerMapTable)
	// Create beneficiary_code_map SQLite table
	sqliteinteract.CreateBeneficiaryCodeTable(dbSqlite, beneficiaryCodeTable)

	// item_price_credit process --------------------------------------------------------------
	// join ledgers in SQLite
	// (i) create ledgerMapKey from baa into SQLite, (ii) join ledgerMapKey to item_price_credit_oms
	// BEFORE CODING check if (i) you cannot map benef_code right await with ledger map,
	// (ii) you cannot join ledgers and benef_code to item_price_oms AND item_price_credit_oms at the same time
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
