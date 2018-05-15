package main

import (
	"github.com/thomas-bamilo/financebooking/dbinteract/omsinteract"
	"github.com/thomas-bamilo/financebooking/dbinteract/scinteract"
	"github.com/thomas-bamilo/sql/connectdb"
)

func main() {

	dbSc := connectdb.ConnectToSc()
	sellerCenterTable := scinteract.GetSellerCenterData(dbSc)

	omsIDSalesOrderItemList := sellerCenterTable[0].OmsIDSalesOrderItem
	for i := 1; i < len(sellerCenterTable); i++ {
		omsIDSalesOrderItemList += `,` + sellerCenterTable[i].OmsIDSalesOrderItem
	}

	dbOms := connectdb.ConnectToOms()
	ledgerMapKeyTable := omsinteract.GetLedgerMapKey(dbOms, omsIDSalesOrderItemList)

}
